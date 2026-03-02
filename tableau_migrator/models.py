from dataclasses import dataclass
from pathlib import Path
from typing import Optional, TypedDict


@dataclass
class DatasourceContext:
    twb_name: str
    base_name: str
    connection_type: str
    caption: str
    hyper_path: Path
    published_name: Optional[str] = None
    published_id: Optional[str] = None
    content_url: Optional[str] = None


@dataclass
class DatasourceDef(TypedDict):
    datasource_class: str
    connection_class: str
    caption: str
    has_extract: bool
