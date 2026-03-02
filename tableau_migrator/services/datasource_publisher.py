import logging
import shutil
import tableauserverclient as TSC
import tempfile
import zipfile

from pathlib import Path
from typing import Dict, List
from types import SimpleNamespace

from tableau_migrator.config import RunMode, Settings
from tableau_migrator.utils.xml_utils import safe_name


logger = logging.getLogger(__name__)

CredentialsMap = Dict[str, List[dict]]


class DatasourcePublisher:
    def __init__(
        self, settings: Settings, server: TSC.Server, credentials_map: CredentialsMap
    ):
        self.settings = settings
        self.server = server
        self.credentials_map = credentials_map
        self.cache: dict[tuple[str, str], TSC.DatasourceItem] = {}

    def package(self, tds_path: Path, hyper_path: Path, output_path: Path) -> Path:
        if tds_path.suffix != ".tds":
            raise ValueError("tds_path must point to a .tds file.")
        if hyper_path.suffix != ".hyper":
            raise ValueError("hyper_path must point to a .hyper file.")

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            data_extracts_dir = tmpdir_path / "Data" / "Extracts"
            data_extracts_dir.mkdir(parents=True, exist_ok=True)

            tds_target = tmpdir_path / tds_path.name
            hyper_target = data_extracts_dir / hyper_path.name

            shutil.copy2(tds_path, tds_target)
            shutil.copy2(hyper_path, hyper_target)

            with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as z:
                for file in tmpdir_path.rglob("*"):
                    if file.is_file():
                        arcname = file.relative_to(tmpdir_path)
                        z.write(file, arcname)

        return output_path

    def publish(
        self,
        wb: TSC.WorkbookItem,
        ds_def: dict,
        hyper_path: Path,
        index: int,
        tds_path: Path,
        ds_name: str = None,
        target_project_id: str = None,
    ) -> TSC.DatasourceItem:
        ctype = ds_def["connection_class"]
        if ds_name is None:
            caption = safe_name(ds_def.get("caption") or f"{ctype} {index}")
            ds_name = f"{wb.name} - {caption}"
        project_id = target_project_id or wb.project_id

        cache_key = (project_id, ds_name)
        if cache_key in self.cache:
            logger.debug("Using cached datasource for %s", ds_name)
            return self.cache[cache_key]

        tdsx_path: Path | None = None

        try:
            # ---------------------------
            # DRY RUN
            # ---------------------------
            if self.settings.run_mode == RunMode.DRY_RUN:
                logger.info(
                    "[DRY RUN] Would publish datasource: "
                    "Name=%s Project=%s Extract=%s Connection=%s",
                    ds_name,
                    wb.project_id,
                    hyper_path.name,
                    ctype,
                )

                if ctype in self.credentials_map:
                    logger.info(
                        "[DRY RUN] Credentials available " "for connection type '%s'",
                        ctype,
                    )
                else:
                    logger.warning(
                        "[DRY RUN] No credentials found for " "connection type '%s'",
                        ctype,
                    )

                logger.info(
                    "Datasource [%s] skipped per run mode: %s",
                    ds_name,
                    self.settings.run_mode.value,
                )
                return SimpleNamespace(id=f"DRY-RUN-{ds_name}", name=ds_name)

            # ---------------------------
            # Package TDSX
            # ---------------------------
            tdsx_path = self.package(
                tds_path=tds_path,
                hyper_path=hyper_path,
                output_path=self.settings.work_dir / f"{ds_name}.tdsx",
            )

            ds_item = TSC.DatasourceItem(project_id=project_id, name=ds_name)

            logger.info(
                "Publishing datasource '%s' to project ID '%s'",
                ds_name,
                project_id,
            )
            published_ds = self.server.datasources.publish(
                ds_item, tdsx_path, TSC.Server.PublishMode.Overwrite
            )

            # Set datasource owner to match workbook owner
            published_ds.owner_id = wb.owner_id
            self.server.datasources.update(published_ds)

            logger.info(
                "Published datasource '%s' (ID: %s)",
                published_ds.name,
                published_ds.id,
            )

            # ---------------------------
            # Update Credentials (REST API)
            # ---------------------------
            try:
                self.server.datasources.populate_connections(published_ds)
                creds_for_type = self.credentials_map.get(ctype, [])

                if not creds_for_type:
                    logger.warning(
                        "No credentials configured for connection type '%s' "
                        "(datasource: %s)",
                        ctype,
                        published_ds.name,
                    )
                else:
                    cred_lookup = {
                        c["username"]: c for c in creds_for_type if c.get("username")
                    }

                    import requests
                    base = (
                        f"{self.settings.server_url}/api/"
                        f"{self.server.version}/sites/{self.server.site_id}"
                    )
                    api_headers = {
                        "X-Tableau-Auth": self.server.auth_token,
                        "Content-Type": "application/xml",
                    }

                    # Also build a server-address fallback lookup
                    server_lookup = {}
                    for c in creds_for_type:
                        srv_key = c.get("server", "").strip().lower()
                        if srv_key and srv_key not in server_lookup:
                            server_lookup[srv_key] = c

                    for conn in published_ds.connections:
                        cred = cred_lookup.get(conn.username)
                        if cred is None:
                            # Fallback: match by server address
                            conn_srv = (conn.server_address or "").strip().lower()
                            cred = server_lookup.get(conn_srv)
                        if cred is None:
                            logger.debug(
                                "No credential match for connection username '%s' "
                                "server '%s' in datasource '%s'",
                                conn.username,
                                conn.server_address,
                                published_ds.name,
                            )
                            continue

                        # Use REST API directly — TSC's update_connection
                        # is blocked by Tableau Cloud's feature gate.
                        body = (
                            '<tsRequest>'
                            '<connection'
                            f' userName="{cred["username"]}"'
                            f' password="{cred["password"]}"'
                            ' embedPassword="true"'
                            '/>'
                            '</tsRequest>'
                        )
                        url = (
                            f"{base}/datasources/{published_ds.id}"
                            f"/connections/{conn.id}"
                        )
                        resp = requests.put(url, headers=api_headers, data=body)

                        if resp.status_code == 200:
                            logger.info(
                                "Embedded credentials for datasource '%s' "
                                "(user=%s) via REST API.",
                                published_ds.name,
                                cred["username"],
                            )
                        else:
                            logger.warning(
                                "REST API credential update returned HTTP %d "
                                "for datasource '%s': %s",
                                resp.status_code,
                                published_ds.name,
                                resp.text[:300],
                            )
            except Exception as cred_err:
                logger.warning(
                    "Credential update failed for datasource '%s': %s. "
                    "The datasource was still published successfully.",
                    published_ds.name,
                    str(cred_err)[:200],
                )

            self.cache[cache_key] = published_ds
            return published_ds

        finally:
            # ---------------------------
            # Cleanup temporary files
            # ---------------------------
            if self.settings.run_mode != RunMode.DRY_RUN:
                for path in (tds_path, tdsx_path):
                    if path and path.exists():
                        try:
                            path.unlink()
                            logger.debug("Cleaned up temp file: %s", path)
                        except Exception as e:
                            logger.warning(
                                "Failed to clean up temp file %s: %s", path, e
                            )
