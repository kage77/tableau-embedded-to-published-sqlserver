import logging
import os
import tableauserverclient as TSC
import zipfile

from lxml import etree as ET
from pathlib import Path

from tableau_migrator.config import Settings
from tableau_migrator.utils.xml_utils import replace_string
from tableau_migrator.models import DatasourceContext


logger = logging.getLogger(__name__)


class WorkbookUpdater:
    def __init__(self, settings: Settings, server: TSC.Server):
        self.settings = settings
        self.server = server

    def rebind(
        self,
        root: ET.Element,
        server_url: str,
        datasources: dict[str, DatasourceContext],
    ) -> ET.Element:
        # rebind helper functions
        ESCAPE_CHARS = ("<", ">", "&", "'", "\n")

        def _needs_cdata(text: str) -> bool:
            if text is None:
                return False
            if text == "Æ\n":
                return False
            return any(ch in text for ch in ESCAPE_CHARS)
        
        def _wrap_cdata(elem: ET.Element) -> None:
            if _needs_cdata(elem.text):
                elem.text = ET.CDATA(elem.text)
                logger.debug(f"Wrapped text in CDATA for element <{elem.tag}>")
            if _needs_cdata(elem.tail):
                elem.tail = ET.CDATA(elem.tail)
                logger.debug(f"Wrapped tail in CDATA for element <{elem.tag}>")

        def _update_datasource_element(ds: ET.Element) -> tuple[str, str] | None:
            old_name = ds.get("name")
            if not old_name or "." not in old_name:
                return
            new_uuid = old_name.split(".", 1)[1]
            new_name = f"sqlproxy.{new_uuid}"
            ds.set("name", new_name)
            logger.info(f"Updated datasource name from '{old_name}' to '{new_name}'")
            return old_name, new_name

        def _update_repo_location(
            ds: ET.Element, repo_id: str, published_name: str
        ) -> None:
            repo_elem = ds.find("repository-location")
            if repo_elem is not None:
                ds.remove(repo_elem)

            if repo_id:
                repo_elem = ET.Element(
                    "repository-location",
                    {
                        "derived-from": f"{self.settings.server_url}/t/{self.settings.site_id}/datasources/{repo_id}?rev=1.0",
                        "id": repo_id,
                        "path": f"/t/{self.settings.site_id}/datasources",
                        "revision": "1.0",
                        "site": self.settings.site_id,
                    },
                )
                first_non_manifest = 0
                for i, child in enumerate(list(ds)):
                    if child.tag != "document-format-change-manifest":
                        first_non_manifest = i
                        break
                ds.insert(first_non_manifest, repo_elem)
                logger.info(
                    f"Added repository-location for datasource '{published_name}' with id={repo_id}"
                )

        def _update_connection_element(
            ds: ET.Element, content_url: str, name: str, published_name: str
        ) -> None:
            conn = ds.find(".//connection")
            conn.set("class", "sqlproxy")
            conn.set("channel", "https")
            conn.set("dbname", content_url)
            conn.set("directory", "/dataserver")
            conn.set("port", "443")
            conn.set("server", self.settings.server_url)
            conn.set("server-oauth", "")
            conn.set("workgroup-auth-mode", "prompt")

            named_conn = ds.find(".//named-connection/connection")
            if named_conn is not None:
                conn.set("username", named_conn.get("username"))

            logger.info(
                f"Updated connection for database '{name}' -> '{published_name}' "
                f"(server={server_url}, dbname={content_url})"
            )

        def _update_relation_elements(ds: ET.Element) -> None:
            relation_keys = [
                ".//relation",
                ".//_.fcp.ObjectModelEncapsulateLegacy.true...relation",
                ".//_.fcp.ObjectModelEncapsulateLegacy.false...relation",
            ]
            for rel_key in relation_keys:
                relations = ds.findall(rel_key)
                if relations is not None:
                    for rel in relations:
                        if rel.get("type") == "table":
                            rel.attrib.pop("connection", None)
                            rel.set("name", "sqlproxy")
                            rel.set("table", "[sqlproxy]")

                        # Handle Custom SQL relations by converting them to table relations pointing to sqlproxy
                        if rel.get("type") == "text":
                            rel.attrib.pop("connection", None)
                            rel.set("type", "table")
                            rel.set("name", "sqlproxy")
                            rel.set("table", "[sqlproxy]")
                            # Clear Custom SQL text since it's no longer valid after repointing to sqlproxy
                            rel.text = None
            logger.info(
                f"Updated relation elements to point to 'sqlproxy' for datasource"
            )

        def _remove_named_connections(ds: ET.Element) -> None:
            for parent in ds.findall(".//named-connections/.."):
                for nc in parent.findall("named-connections"):
                    parent.remove(nc)
            logger.info(f"Removed named connections for datasource")

        def _remove_cols_element(ds: ET.Element) -> None:
            for parent in ds.findall(".//cols/.."):
                for cols in parent.findall("cols"):
                    parent.remove(cols)
            logger.info(f"Removed 'cols' elements for datasource")

        def _remove_metadata_element(ds: ET.Element) -> None:
            for parent in ds.findall(".//metadata-records/.."):
                for md in parent.findall("metadata-records"):
                    parent.remove(md)
            logger.info(f"Removed metadata records for datasource")

        def _remove_extract_elements(ds: ET.Element) -> None:
            for parent in ds.findall(".//extract/.."):
                for extract in parent.findall("extract"):
                    parent.remove(extract)
            logger.info(f"Removed embedded extract elements for datasource")

        def _update_object_graphs(ds: ET.Element) -> None:
            for obj in ds.findall(".//object"):
                for props in obj.findall("properties"):
                    if props.get("context") == "extract":
                        obj.remove(props)
                rel = obj.find('.//properties[@context=""]/relation')
                if rel is not None:
                    rel.attrib.pop("connection", None)
                    rel.set("name", "sqlproxy")
                    rel.set("table", "[sqlproxy]")
            logger.info(
                f"Updated object graphs to remove extract properties and point to 'sqlproxy'"
            )

        # primary rebind function
        updated_count = 0
        rename_map = {}

        for ds in root.findall(".//datasource[@inline='true']"):
            name = ds.get("name")
            if not name:
                continue

            ctx = datasources.get(name)
            if not ctx:
                continue

            repo_id = ctx.content_url
            published_name = ctx.published_name

            result = _update_datasource_element(ds)
            if result:
                old_name, new_name = result
                rename_map[old_name] = new_name

            _update_repo_location(ds, repo_id, published_name)
            _update_connection_element(ds, ctx.content_url, name, published_name)
            _update_relation_elements(ds)
            _remove_named_connections(ds)
            _remove_cols_element(ds)
            _remove_metadata_element(ds)
            _remove_extract_elements(ds)
            _update_object_graphs(ds)

            updated_count += 1

        # After all updates, perform a final pass to replace any remaining references
        # to old datasource names
        for elem in root.iter():
            for attr, val in list(elem.attrib.items()):
                new_val = val
                for old, new in rename_map.items():
                    new_val = replace_string(new_val, old, new)
                if new_val != val:
                    elem.set(attr, new_val)
            if elem.text:
                new_text = elem.text
                for old, new in rename_map.items():
                    new_text = replace_string(new_text, old, new)
                if new_text != elem.text:
                    elem.text = new_text
            _wrap_cdata(elem)

        return root

    def remove_embedded_extracts(self, published_hyper_paths: list[Path | str]) -> None:
        for path in published_hyper_paths:
            p = Path(path)
            if p.exists():
                try:
                    p.unlink()
                    logger.debug(f"Removed embedded extract: {p}")
                except Exception as e:
                    logger.warning(f"Failed to remove {p}: {e}")

    def repackage(self, output_twbx_path: Path | str) -> None:
        output_twbx_path = Path(output_twbx_path)
        with zipfile.ZipFile(output_twbx_path, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _, files in os.walk(self.settings.extract_dir):
                for file in files:
                    full_path = Path(root) / file
                    arcname = full_path.relative_to(self.settings.extract_dir)
                    z.write(full_path, arcname)
        logger.info(f"Repackaged workbook: {output_twbx_path}")
