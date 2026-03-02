"""
"""
import csv
import logging
import shutil
import time
import tableauserverclient as TSC

from datetime import datetime, timezone
from pathlib import Path
from lxml import etree as ET
from typing import Dict, Optional

from tableau_migrator.config import RunMode, Settings
from tableau_migrator.utils.csv_utils import ensure_csv, append_csv

from tableau_migrator.models import DatasourceContext, DatasourceDef
from tableau_migrator.services.datasource_publisher import DatasourcePublisher
from tableau_migrator.services.metadata_engine import MetadataEngine
from tableau_migrator.services.workbook_extractor import WorkbookExtractor
from tableau_migrator.services.workbook_updater import WorkbookUpdater


logger = logging.getLogger(__name__)


class WorkbookMigrationRunner:
    def __init__(self, settings: Settings, server: TSC.Server):
        self.settings = settings
        self.server = server
        self.metadata = MetadataEngine(server)
        self.extractor = WorkbookExtractor(settings)
        self.updater = WorkbookUpdater(settings, server)

        # ── EXTENSION 2: DS dedup ──
        # Cache of published DS on Cloud: {ds_name: TSC.DatasourceItem}
        self.existing_published_ds: Dict[str, TSC.DatasourceItem] = {}

        # Project ID for "Published Data Sources"
        self.published_ds_project_id: Optional[str] = None

        # ── EXTENSION 3: SQL credentials from config.py ──
        self.sql_credentials = self._load_sql_credentials()

        # Build a credentials map compatible with the partner script's format
        self.credentials = self._build_credentials_map()

        ensure_csv(
            settings.results_csv,
            [
                "timestamp",
                "workbook_luid",
                "workbook_name",
                "status",
                "run_mode",
                "error",
            ],
        )
        ensure_csv(
            settings.mapping_csv,
            [
                "workbook",
                "twb_datasource",
                "hyper_file",
                "connection_type",
                "published_ds_name",
                "published_ds_id",
                "run_mode",
            ],
        )

    @staticmethod
    def _load_sql_credentials() -> dict:
        """Load SQL credentials from config.py if available."""
        try:
            from config import SQL_SERVER_CONNECTIONS
            return SQL_SERVER_CONNECTIONS
        except (ImportError, AttributeError):
            logger.warning("No SQL_SERVER_CONNECTIONS found in config.py")
            return {}

    def _build_credentials_map(self) -> dict:
        """Build a credentials map in the partner script format from config.py."""
        creds_map = {}
        for (server, db), cred in self.sql_credentials.items():
            ctype = "sqlserver"
            if ctype not in creds_map:
                creds_map[ctype] = []
            creds_map[ctype].append({
                "dbclass": ctype,
                "server": server,
                "database": db,
                "username": cred.get("username", ""),
                "password": cred.get("password", ""),
            })
        return creds_map

    def _refresh_existing_published_ds(self, project_name: str = None):
        """Load all existing published DS from Cloud for dedup checking.
        Also resolve the project ID for publishing new DS."""
        if project_name is None:
            from config import PUBLISHED_DS_PROJECT_NAME
            project_name = PUBLISHED_DS_PROJECT_NAME
        self.existing_published_ds.clear()

        # Resolve the project ID
        for proj in TSC.Pager(self.server.projects):
            if proj.name == project_name:
                self.published_ds_project_id = proj.id
                break
        if self.published_ds_project_id is None:
            raise RuntimeError(
                f"Project '{project_name}' not found on site. "
                f"Please create it manually (or use an admin PAT with create_project.py) before running migration."
            )
        else:
            logger.info("Using project '%s' (id=%s).", project_name, self.published_ds_project_id)

        for ds in TSC.Pager(self.server.datasources):
            if ds.project_name == project_name:
                self.existing_published_ds[ds.name] = ds
        logger.info(
            "Found %d existing published datasource(s) in '%s'.",
            len(self.existing_published_ds), project_name,
        )

    def _copy_extract_schedule(self, source_workbook_id: str, target_ds_id: str):
        """Copy the workbook's extract refresh schedule to the published DS.

        Uses the REST API directly because TSC's tasks.create serializes
        the schedule_item incorrectly for Tableau Cloud.
        Skips if the DS already has an extract refresh task to avoid duplicates.
        """
        try:
            # Find the cached schedule XML for this workbook
            schedule_xml = self._task_schedule_xml.get(source_workbook_id)
            if schedule_xml is None:
                logger.info("No extract-refresh schedule found for workbook %s.", source_workbook_id)
                return

            import requests
            from xml.etree import ElementTree as StdET
            base = f"{self.settings.server_url}/api/{self.server.version}/sites/{self.server.site_id}"
            headers = {
                "X-Tableau-Auth": self.server.auth_token,
                "Content-Type": "application/xml",
            }

            # Check if the DS already has an extract refresh task
            check_headers = {"X-Tableau-Auth": self.server.auth_token}
            check_resp = requests.get(
                f"{base}/tasks/extractRefreshes?pageSize=1000",
                headers=check_headers,
            )
            if check_resp.status_code == 200:
                ns = {"t": "http://tableau.com/api"}
                tasks_root = StdET.fromstring(check_resp.text)
                for er in tasks_root.findall(".//t:extractRefresh", ns):
                    ds_elem = er.find("t:datasource", ns)
                    if ds_elem is not None and ds_elem.get("id") == target_ds_id:
                        logger.info(
                            "Published DS %s already has an extract schedule — skipping.",
                            target_ds_id,
                        )
                        return

            body = (
                '<tsRequest>'
                '<extractRefresh type="extractRefresh">'
                f'<datasource id="{target_ds_id}" />'
                '</extractRefresh>'
                f'{schedule_xml}'
                '</tsRequest>'
            )

            for attempt in range(3):
                resp = requests.post(
                    f"{base}/tasks/extractRefreshes",
                    headers=headers, data=body,
                )
                if resp.status_code == 200:
                    logger.info(
                        "Copied extract schedule to published DS %s (from workbook %s).",
                        target_ds_id, source_workbook_id,
                    )
                    return
                else:
                    if attempt < 2:
                        logger.debug("Schedule copy attempt %d failed (HTTP %d), retrying in 3s...",
                                     attempt + 1, resp.status_code)
                        time.sleep(3)
                    else:
                        logger.warning(
                            "Extract schedule copy failed for DS %s: HTTP %d - %s",
                            target_ds_id, resp.status_code, resp.text[:300],
                        )
        except Exception as exc:
            logger.warning("Extract schedule copy skipped for DS %s: %s", target_ds_id, exc)

    def _is_custom_sql(self, twb_path: Path, ds_name: str) -> bool:
        """Check if a DS uses Custom SQL by inspecting the TWB XML."""
        tree = ET.parse(twb_path, ET.XMLParser(remove_blank_text=True))
        root = tree.getroot()
        for ds in root.findall(".//datasource"):
            name = ds.get("name", "")
            if name == ds_name or name.startswith(ds_name):
                # Check for text-type relation (Custom SQL)
                relation_keys = [
                    ".//relation[@type='text']",
                    ".//_.fcp.ObjectModelEncapsulateLegacy.true...relation[@type='text']",
                    ".//_.fcp.ObjectModelEncapsulateLegacy.false...relation[@type='text']",
                ]
                for key in relation_keys:
                    if ds.findall(key):
                        return True
        return False

    def log_section(self, title: str):
        logger.info("")
        logger.info("=" * 80)
        logger.info(title)
        logger.info("=" * 80)

    def already_processed(self, workbook_id: str) -> bool:
        if self.settings.force_reprocess:
            return False
        if not self.settings.results_csv.exists():
            return False
        with self.settings.results_csv.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if (
                    row["workbook_luid"] == workbook_id
                    and row["status"] == "SUCCESS"
                    and row["run_mode"] == self.settings.run_mode.value
                ):
                    return True
        return False

    def log_discovery(
        self,
        ds_defs: Dict[str, DatasourceDef],
        hyper_map: Dict[str, str],
        wb: TSC.WorkbookItem,
    ) -> None:
        logger.info("Embedded Datasources discovered:")
        for ds, meta in ds_defs.items():
            logger.info(
                f" - {ds}"
                f"(type={meta['connection_class']}, "
                f"caption={meta['caption']})"
            )
        logger.info("Embedded Extracts discovered:")
        for ds, path in hyper_map.items():
            logger.info(f" - {ds} -> {Path(path).name}")

        logger.info("Workbook connections discovered:")
        for conn in wb.connections:
            logger.info(
                f" - conn_id={conn.id}, "
                f"type={conn.connection_type}, "
                f"user={conn.username}, "
                f"server={conn.server_address}"
            )

    def discover_publishable_datasources(
        self,
        twb_path: Path,
        ds_defs: Dict[str, DatasourceDef],
        hyper_map: Dict[str, str],
        wb_name: str,
    ) -> Dict[str, DatasourceContext]:
        datasources = {}

        for ds_name, ds_def in ds_defs.items():
            ctype = ds_def["connection_class"]
            caption = ds_def["caption"]

            if (
                self.settings.allowed_connection_types
                and ctype not in self.settings.allowed_connection_types
            ):
                logger.debug(
                    f"Skipping datasource {ds_name}: connection type '{ctype}' "
                    f"not in allowed list {self.settings.allowed_connection_types}"
                )
                continue

            base_name = ds_name[:-6]
            if base_name not in hyper_map:
                # ── EXTENSION: also try underscore-based names ──
                alt_base = ds_name.replace(".", "_").rstrip("_")
                if alt_base not in hyper_map:
                    logger.warning(
                        "No hyper file found for datasource '%s' (base='%s')", ds_name, base_name
                    )
                    continue

            tds_path = self.settings.work_dir / f"{ds_name}.tds"
            self.extractor.twb_to_tds(
                path=twb_path, ds_name=base_name, output_path=tds_path
            )

            datasources[ds_name] = DatasourceContext(
                twb_name=ds_name,
                base_name=base_name,
                connection_type=ctype,
                caption=caption,
                hyper_path=hyper_map[base_name],
            )

        return datasources

    def publish_datasources(
        self,
        wb: TSC.WorkbookItem,
        datasources: Dict[str, DatasourceContext],
        publisher: DatasourcePublisher,
        twb_path: Path,
    ) -> None:
        for index, (ds_name, ctx) in enumerate(datasources.items(), 1):
            try:
                # ── EXTENSION 1: Custom SQL DS naming ──
                is_csql = self._is_custom_sql(twb_path, ctx.base_name)

                # Build the DS publish name
                from tableau_migrator.utils.xml_utils import safe_name
                caption = safe_name(ctx.caption or f"{ctx.connection_type} {index}")
                if is_csql:
                    ds_publish_name = f"{wb.name} - {caption}"
                    logger.info(
                        "Custom SQL datasource detected: '%s' -> '%s'",
                        ctx.twb_name, ds_publish_name,
                    )
                else:
                    ds_publish_name = caption

                # ── EXTENSION 2: DS dedup ──
                existing = self.existing_published_ds.get(ds_publish_name)
                if existing is not None:
                    logger.info(
                        "Datasource '%s' already exists (id=%s), reusing.",
                        ds_publish_name, existing.id,
                    )
                    ctx.published_name = existing.name
                    ctx.published_id = existing.id
                    ctx.content_url = existing.content_url

                    # Still copy extract schedule even when reusing DS
                    self._copy_extract_schedule(wb.id, existing.id)

                    append_csv(
                        self.settings.mapping_csv,
                        [
                            wb.name,
                            ctx.twb_name,
                            Path(ctx.hyper_path).name,
                            ctx.connection_type,
                            ctx.published_name,
                            ctx.published_id,
                            self.settings.run_mode.value,
                        ],
                    )
                    continue

                # Publish with the computed name to Published Data Sources project
                logger.info(f"Publishing datasource '{ds_publish_name}'...")

                published_ds = publisher.publish(
                    wb,
                    {
                        "connection_class": ctx.connection_type,
                        "caption": ctx.caption,
                    },
                    ctx.hyper_path,
                    index,
                    self.settings.work_dir / f"{ctx.twb_name}.tds",
                    ds_name=ds_publish_name,
                    target_project_id=self.published_ds_project_id,
                )

                ctx.published_name = published_ds.name
                ctx.published_id = published_ds.id
                ctx.content_url = published_ds.content_url

                # Copy extract refresh schedule from workbook to published DS
                self._copy_extract_schedule(wb.id, published_ds.id)

                # Add to cache for dedup
                self.existing_published_ds[published_ds.name] = published_ds

                append_csv(
                    self.settings.mapping_csv,
                    [
                        wb.name,
                        ctx.twb_name,
                        Path(ctx.hyper_path).name,
                        ctx.connection_type,
                        ctx.published_name,
                        ctx.published_id,
                        self.settings.run_mode.value,
                    ],
                )

            except Exception as e:
                logger.error(
                    f"Failed to publish datasource for {wb.name} ({ctx.twb_name}): {e}"
                )

    def process_workbook(self, wb_id: str, publisher: DatasourcePublisher) -> None:
        twbx_path = None
        try:
            wb = self.server.workbooks.get_by_id(wb_id)

            if self.already_processed(wb.id):
                logger.info(f"Skipping already-processed workbook: {wb.name}")
                return

            # ── EXTENSION 3: Skip workbooks already fully migrated ──
            self.server.workbooks.populate_connections(wb)
            ds_types = [c.connection_type for c in wb.connections]
            if ds_types and all(t == "sqlproxy" for t in ds_types):
                logger.info(
                    "Skipping workbook '%s': all %d DS are already published (sqlproxy).",
                    wb.name, len(ds_types),
                )
                return

            self.log_section(f"PROCESSING WORKBOOK - {wb.name}")

            logger.info(f"Getting a list of hidden views for workbook '{wb.name}'...")
            hidden_views = self.metadata.get_hidden_views(wb.id)

            logger.info(f"Downloading and extracting workbook '{wb.name}'...")
            twbx_path = self.server.workbooks.download(
                wb.id, filepath=self.settings.work_dir
            )
            if not isinstance(twbx_path, Path):
                twbx_path = Path(twbx_path)

            self.extractor.unzip(twbx_path)

            twb_path = self.extractor.find_file(".twb")
            tree = ET.parse(twb_path, ET.XMLParser(remove_blank_text=True))
            root = tree.getroot()

            ds_defs = self.extractor.parse_twb_datasources(root)
            hyper_map = self.extractor.map_hyper_files()

            self.log_section(f"DISCOVERY - {wb.name}")
            self.log_discovery(ds_defs, hyper_map, wb)

            self.log_section(f"MAPPING - {wb.name}")
            datasources = self.discover_publishable_datasources(
                twb_path, ds_defs, hyper_map, wb.name
            )

            self.log_section(f"PUBLISHING DATASOURCES - {wb.name}")
            logger.info(
                f"Publishing {len(datasources)} datasources for workbook '{wb.name}'..."
            )
            self.publish_datasources(wb, datasources, publisher, twb_path)

            # Rebind workbook to the published datasources
            self.log_section(f"REBINDING WORKBOOK - {wb.name}")
            logger.info(f"Rebinding workbook '{wb.name}' to published datasources...")
            root = self.updater.rebind(
                root,
                self.settings.server_url.replace("https://", "").rstrip("/"),
                datasources,
            )

            # Hide non-dashboard sheets so only dashboards are visible
            dashboard_names = set()
            for dash in root.iter("dashboard"):
                dname = dash.get("name")
                if dname:
                    dashboard_names.add(dname)
            if dashboard_names:
                for window in root.iter("window"):
                    wname = window.get("name", "")
                    wclass = window.get("class", "")
                    if wclass == "dashboard" or wname in dashboard_names:
                        continue
                    if window.get("hidden") != "true":
                        window.set("hidden", "true")
                        logger.info("Hiding non-dashboard sheet: '%s'", wname)

            tree.write(
                twb_path, encoding="utf-8", xml_declaration=True, pretty_print=True
            )

            published_hyper_paths = [ctx.hyper_path for ctx in datasources.values()]
            self.updater.remove_embedded_extracts(published_hyper_paths)
            self.updater.repackage(twbx_path)

            if self.settings.run_mode == RunMode.FULL_RUN:
                try:
                    self.log_section(f"PUBLISHING UPDATED WORKBOOK - {wb.name}")
                    wb.hidden_views = hidden_views
                    self.server.workbooks.publish(
                        wb, twbx_path, TSC.Server.PublishMode.Overwrite
                    )
                    logger.info(f"Workbook '{wb.name}' has successfully republished.")

                except Exception as e:
                    logger.error(
                        f"Failed to republish workbook '{wb.name}': {e}"
                    )
                    append_csv(
                        self.settings.results_csv,
                        [
                            datetime.now(timezone.utc).isoformat(),
                            wb.id,
                            wb.name,
                            "FAILED",
                            self.settings.run_mode.value,
                            str(e),
                        ],
                    )
                    return

            else:
                logger.info(f"[DRY RUN] Workbook '{wb.name}' would be republished.")

            self.log_section(f"SUMMARY — {wb.name}")

            logger.info(
                f"Workbook published: {'YES' if self.settings.run_mode == RunMode.FULL_RUN else 'NO'}"
            )

            # Remind user about manual Private Network step for new DS
            new_ds_names = [
                ctx.published_name for ctx in datasources.values()
                if ctx.published_id
            ]
            if new_ds_names:
                logger.info("")
                logger.info(
                    "ACTION REQUIRED: Set each new published datasource to 'Private network' "
                    "in Tableau Cloud (Edit Connection > Network type > Private network):"
                )
                for name in new_ds_names:
                    logger.info("  - %s", name)
            append_csv(
                self.settings.results_csv,
                [
                    datetime.now(timezone.utc).isoformat(),
                    wb.id,
                    wb.name,
                    "SUCCESS",
                    self.settings.run_mode.value,
                    "",
                ],
            )

        except Exception as e:
            logger.exception(f"Workbook {wb_id} failed to process")
            append_csv(
                self.settings.results_csv,
                [
                    datetime.now(timezone.utc).isoformat(),
                    wb_id,
                    "",
                    "FAILED",
                    self.settings.run_mode.value,
                    str(e),
                ],
            )

        finally:
            if twbx_path and twbx_path.exists():
                twbx_path.unlink()
            shutil.rmtree(self.settings.extract_dir, ignore_errors=True)
            self.settings.extract_dir.mkdir(parents=True, exist_ok=True)

    def run(self, workbook_ids: Optional[list[str]] = None) -> None:
        """Run migration on the given workbook IDs.
        
        If workbook_ids is None, loads from the Excel file (partner script behavior).
        If workbook_ids is provided, uses those directly (CLI usage).
        """
        if workbook_ids is None:
            from tableau_migrator.utils.file_utils import load_workbook_luids
            workbook_ids = load_workbook_luids(
                self.settings.workbook_xlsx, self.settings.workbook_sheet
            )

        # ── Load extract refresh tasks FIRST, before any workbook processing ──
        # Workbook extract tasks may be deleted/modified during migration,
        # so we must capture them now.
        # We use the REST API directly to get the raw XML with <schedule> elements,
        # because TSC's tasks.create can't serialize them correctly for Cloud.
        self._task_schedule_xml = {}
        try:
            import requests
            from xml.etree import ElementTree as StdET
            base = f"{self.settings.server_url}/api/{self.server.version}/sites/{self.server.site_id}"
            headers = {"X-Tableau-Auth": self.server.auth_token}
            resp = requests.get(f"{base}/tasks/extractRefreshes?pageSize=1000", headers=headers)
            if resp.status_code == 200:
                ns = {"t": "http://tableau.com/api"}
                root = StdET.fromstring(resp.text)
                # Structure: <task><extractRefresh><schedule>...</schedule><workbook id="..."/></extractRefresh></task>
                for er in root.findall(".//t:extractRefresh", ns):
                    wb_elem = er.find("t:workbook", ns)
                    if wb_elem is None:
                        continue
                    wb_id = wb_elem.get("id")
                    schedule_elem = er.find("t:schedule", ns)
                    if schedule_elem is None:
                        continue
                    # Serialize schedule XML, strip namespace for clean POST body
                    schedule_xml = StdET.tostring(schedule_elem, encoding="unicode")
                    import re
                    schedule_xml = re.sub(r'\s*xmlns:[^=]+="[^"]*"', '', schedule_xml)
                    schedule_xml = re.sub(r'\s*xmlns="[^"]*"', '', schedule_xml)
                    schedule_xml = re.sub(r'ns\d+:', '', schedule_xml)
                    if wb_id not in self._task_schedule_xml:
                        self._task_schedule_xml[wb_id] = schedule_xml
                        logger.info(
                            "Captured extract schedule for workbook %s: %s",
                            wb_id, schedule_xml[:200],
                        )
            logger.info(
                "Loaded extract schedules for %d workbook(s).", len(self._task_schedule_xml)
            )
        except Exception as exc:
            logger.warning("Failed to load extract refresh tasks: %s", exc)

        # ── EXTENSION 2: Load existing published DS for dedup ──
        self._refresh_existing_published_ds()

        publisher = DatasourcePublisher(self.settings, self.server, self.credentials)
        failures = []

        for wb_id in workbook_ids:
            try:
                self.process_workbook(wb_id, publisher)
            except Exception as e:
                logger.exception(f"Unexpected error processing workbook {wb_id}: {e}")
                failures.append(
                    {"workbook": wb_id, "datasource": "unknown", "error": str(e)}
                )

        if failures:
            logger.warning(f"\n---- Migration Summary: {len(failures)} failures ----")
            for f in failures:
                logger.warning(f"{f['workbook']} | {f['datasource']} | {f['error']}")
        else:
            logger.info(
                "\n---- Migration Summary: All workbooks processed successfully ----"
            )
