import logging
import zipfile

from copy import deepcopy
from lxml import etree as ET
from pathlib import Path
from typing import Optional, Dict

from tableau_migrator.config import Settings


logger = logging.getLogger(__name__)


class WorkbookExtractor:
    def __init__(self, settings: Settings):
        self.settings = settings

    def unzip(self, path: Path) -> None:
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(self.settings.extract_dir)

    def find_file(self, suffix: str) -> Optional[Path]:
        for file in self.settings.extract_dir.rglob(f"*{suffix}"):
            return file
        return None

    def parse_twb_datasources(self, root: ET.Element) -> Dict[str, dict]:
        ds_map: Dict[str, dict] = {}

        for ds in root.findall(".//datasource"):
            name = ds.get("name")
            if name is None:
                continue

            ds_conn = ds.find("connection")
            if ds_conn is None:
                continue

            datasource_class = ds_conn.get("class", "").lower()

            named_conn = ds_conn.find(".//named-connection/connection")
            connection_class = (
                named_conn.get("class", "").lower()
                if named_conn is not None
                else datasource_class
            )

            caption = ds.get("caption", "")
            has_extract = ds.find(".//extract") is not None

            ds_map[name] = {
                "datasource_class": datasource_class,
                "connection_class": connection_class,
                "caption": caption,
                "has_extract": has_extract,
            }

        return ds_map

    def map_hyper_files(self) -> Dict[str, Path]:
        hyper_map: Dict[str, Path] = {}

        for file in self.settings.extract_dir.rglob("federated_*.hyper"):
            base = file.stem
            ds_name = base.replace("federated_", "federated.", 1)
            hyper_map[ds_name] = file

        return hyper_map

    def twb_to_tds(self, path: Path, ds_name: str, output_path: Path) -> Path:
        def _correct_custom_sql_cdata(root: ET.Element) -> None:
            relation_keys = [
                ".//relation",
                ".//_.fcp.ObjectModelEncapsulateLegacy.true...relation",
                ".//_.fcp.ObjectModelEncapsulateLegacy.false...relation",
            ]
            for rel_key in relation_keys:
                relations = root.findall(rel_key)
                if relations is not None:
                    for rel in relations:
                        # Convert Custom SQL to CDATA
                        if rel.text:
                            rel.text = ET.CDATA(rel.text)

        ET.register_namespace("user", "http://www.tableausoftware.com/xml/user")
        ET.register_namespace("xml", "http://www.w3.org/XML/1998/namespace")

        tree = ET.parse(path, ET.XMLParser(remove_blank_text=True))
        root = tree.getroot()

        wb_attrib = dict(root.attrib)
        dfcm = root.find("document-format-change-manifest")
        if dfcm is not None:
            dfcm = deepcopy(dfcm)

        for ds in root.findall(".//datasource"):
            name = ds.get("name")
            if name and name.startswith(ds_name):
                new_root = ET.Element(
                    "datasource",
                    {
                        "formatted-name": name,
                        "inline": ds.get("inline", "true"),
                        "source-platform": wb_attrib.get("source-platform", ""),
                        "source-build": wb_attrib.get("source-build", ""),
                        "version": wb_attrib.get("version"),
                        "{http://www.w3.org/XML/1998/namespace}base": self.settings.server_url,
                    },
                )

                if dfcm is not None:
                    new_root.append(dfcm)

                ds_copy = deepcopy(ds)
                ds_copy.attrib.pop("caption", None)
                ds_copy.attrib.pop("name", None)

                repo = ds_copy.find("repository-location")
                if repo is not None:
                    repo.attrib.pop("derived-from", None)
                    repo.set("id", name)
                    repo.set(
                        "path",
                        repo.get("path", "").replace("/workbooks", "/datasources"),
                    )
                    repo.set("revision", "1.0")

                for child in list(ds_copy):
                    new_root.append(child)

                # Correct any custom SQL CDATA fields before writing to file
                _correct_custom_sql_cdata(new_root)

                output_path.parent.mkdir(parents=True, exist_ok=True)
                ET.ElementTree(new_root).write(
                    output_path,
                    encoding="utf-8",
                    xml_declaration=True,
                    pretty_print=True,
                )

                return output_path

        raise ValueError("Datasource %s not found in workbook XML.", ds_name)
