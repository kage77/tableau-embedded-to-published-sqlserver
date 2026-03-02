"""
Usage:
    python migrate_v2.py "Workbook Name 1" "Workbook Name 2" ...
    python migrate_v2.py --all                  # migrate all workbooks on site
    python migrate_v2.py --dry-run "Workbook"   # dry run (no publish)
"""
import argparse
import logging
import os
import sys

from dotenv import load_dotenv
load_dotenv()

# Set env vars from config.py if not already set by .env
from config import SERVER_URL, SITE_NAME, PAT_NAME, PAT_SECRET, SOURCE_PROJECT_NAME, PUBLISHED_DS_PROJECT_NAME

os.environ.setdefault("SERVER_URL", SERVER_URL)
os.environ.setdefault("SITE_ID", SITE_NAME)
os.environ.setdefault("TOKEN_NAME", PAT_NAME)
os.environ.setdefault("TOKEN_VALUE", PAT_SECRET)
os.environ.setdefault("RUN_MODE", "full_run")
os.environ.setdefault("ALLOWED_CONNECTION_TYPES", "sqlserver")

from tableau_migrator.config import Settings
from tableau_migrator.logging_config import configure_logging
from tableau_migrator.migration.runner import WorkbookMigrationRunner
from tableau_migrator.services.tableau_auth import tableau_authenticate

import tableauserverclient as TSC


def resolve_workbook_ids(server: TSC.Server, names: list[str], project_filter: str = None) -> list[str]:
    """Resolve workbook names to IDs, filtering to a specific project.
    
    If project_filter is set, only workbooks in that project are matched.
    This avoids picking up duplicate workbook names from other projects.
    """
    all_wbs = list(TSC.Pager(server.workbooks))
    if project_filter:
        all_wbs = [wb for wb in all_wbs if wb.project_name == project_filter]
        logging.info("Filtered to %d workbooks in project '%s'.", len(all_wbs), project_filter)

    ids = []
    for name in names:
        matches = [wb for wb in all_wbs if wb.name.lower() == name.lower()]
        if not matches:
            logging.error(
                "Workbook '%s' NOT FOUND in project '%s'. "
                "Please publish it to the server first, then re-run this script.",
                name, project_filter or "(all)",
            )
            input(f"  >> Press Enter after uploading '{name}' to retry, or Ctrl+C to abort... ")
            # Refresh workbook list and retry
            all_wbs = list(TSC.Pager(server.workbooks))
            if project_filter:
                all_wbs = [wb for wb in all_wbs if wb.project_name == project_filter]
            matches = [wb for wb in all_wbs if wb.name.lower() == name.lower()]
            if not matches:
                logging.error("Workbook '%s' still not found. Skipping.", name)
                continue
        for m in matches:
            ids.append(m.id)
            logging.info("Resolved: '%s' -> %s (project=%s)", m.name, m.id, m.project_name)
    return ids


def main():
    parser = argparse.ArgumentParser(
        description="Migrate embedded data sources to published (partner script + extensions)."
    )
    parser.add_argument(
        "workbooks",
        nargs="*",
        help="Workbook names to migrate.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Migrate all workbooks on the site.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run — discover and log but don't publish.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reprocessing of already-processed workbooks.",
    )
    args = parser.parse_args()

    if args.dry_run:
        os.environ["RUN_MODE"] = "dry_run"
    if args.force:
        os.environ["FORCE_REPROCESS"] = "true"

    settings = Settings()
    configure_logging(settings.work_dir.parent / "logs")

    logger = logging.getLogger(__name__)
    logger.info("Starting Tableau Migration (v2 — partner script + extensions)")

    server, auth = tableau_authenticate(
        token_name=settings.token_name,
        token_value=settings.token_value,
        portal_url=settings.server_url,
        site_id=settings.site_id,
    )

    with server.auth.sign_in(auth):
        if args.all:
            wb_ids = [
                wb.id for wb in TSC.Pager(server.workbooks)
                if wb.project_name == SOURCE_PROJECT_NAME
            ]
            logger.info("Migrating ALL %d workbooks in project '%s'.", len(wb_ids), SOURCE_PROJECT_NAME)
        elif args.workbooks:
            wb_ids = resolve_workbook_ids(server, args.workbooks, project_filter=SOURCE_PROJECT_NAME)
        else:
            parser.error("Provide workbook names or --all.")
            return

        if not wb_ids:
            logger.error("No workbooks to migrate.")
            return

        runner = WorkbookMigrationRunner(settings, server)
        runner.run(workbook_ids=wb_ids)

    logger.info("Migration process complete.")


if __name__ == "__main__":
    main()
