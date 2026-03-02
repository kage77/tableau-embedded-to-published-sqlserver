"""
Configuration (public-safe).

This repository is intended for open source use. Do not hardcode secrets.

Setup:
- Copy `.env.example` to `.env` and fill in Tableau PAT + site details
- Copy `files/credentials.template.csv` to `files/credentials.csv` and fill in SQL Server credentials
- `.env` and `files/credentials.csv` are gitignored

All values are read from environment variables to keep this repo safe for public GitHub.
"""

from __future__ import annotations

import os
from pathlib import Path


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}. See .env.example")
    return value


# Tableau Cloud / Tableau Server connection
TABLEAU_SERVER_URL = required_env("TABLEAU_SERVER_URL")
TABLEAU_SITE_NAME = os.getenv("TABLEAU_SITE_NAME", "")
TABLEAU_PAT_NAME = required_env("TABLEAU_PAT_NAME")
TABLEAU_PAT_SECRET = required_env("TABLEAU_PAT_SECRET")

# Optional defaults used by the migration workflow
SOURCE_PROJECT_NAME = os.getenv("SOURCE_PROJECT_NAME", "SOURCE_PROJECT")
TARGET_PROJECT_NAME = os.getenv("TARGET_PROJECT_NAME", "PUBLISHED_DATASOURCES_PROJECT")

# Local-only SQL credentials file (do not commit)
SQL_CREDENTIALS_FILE = Path(os.getenv("SQL_CREDENTIALS_FILE", "files/credentials.csv"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
