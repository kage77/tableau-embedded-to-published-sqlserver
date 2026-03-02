import os

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List


class RunMode(str, Enum):
    DRY_RUN = "dry_run"
    PUBLISH_DS_ONLY = "publish_ds_only"
    FULL_RUN = "full_run"


@dataclass
class Settings:
    server_url: str = os.getenv("SERVER_URL")
    site_id: str = os.getenv("SITE_ID")
    token_name: str = os.getenv("TOKEN_NAME")
    token_value: str = os.getenv("TOKEN_VALUE")

    run_mode: RunMode = RunMode(os.getenv("RUN_MODE", "full_run"))
    force_reprocess: bool = False

    allowed_connection_types: List[str] = field(
        default_factory=lambda: [
            x.strip().lower()
            for x in os.getenv("ALLOWED_CONNECTION_TYPES", "").split(",")
            if x.strip()
        ]
    )

    workbook_xlsx: Path = Path("./files/workbook_list.xlsx")
    workbook_sheet: str = "Workbooks"
    credentials_file: Path = Path("./files/credentials.csv")

    work_dir: Path = Path("./files/temp")
    extract_dir: Path = work_dir / "extracted"
    results_csv: Path = Path("./files/migration_results.csv")
    mapping_csv: Path = Path("./files/datasource_mapping.csv")

    def __post_init__(self):
        required = {
            "SERVER_URL": self.server_url,
            "SITE_ID": self.site_id,
            "TOKEN_NAME": self.token_name,
            "TOKEN_VALUE": self.token_value,
        }

        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        self.server_url = self.server_url.rstrip("/")
        # Re-read these at instantiation time (env vars may be set after import)
        self.force_reprocess = os.getenv("FORCE_REPROCESS", "false").lower() == "true"
        self.run_mode = RunMode(os.getenv("RUN_MODE", "full_run"))

    def is_connection_allowed(self, connection_type: str) -> bool:
        if not self.allowed_connection_types:
            return True
        return connection_type.lower() in self.allowed_connection_types
