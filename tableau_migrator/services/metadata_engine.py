import logging
import tableauserverclient as TSC


logger = logging.getLogger(__name__)


class MetadataEngine:
    def __init__(self, server: TSC.Server):
        self.server = server

    def get_hidden_views(self, wb_id: str) -> list[str]:
        hidden_views = []

        query = f"""
            query {{
                workbooks (filter: {{luid: "{wb_id}"}}) {{
                    luid
                    name
                    views {{
                        luid
                        name
                    }}
                }}
            }}"""

        response_data = self.server.metadata.query(
            query=query, variables=None, abort_on_error=True
        )

        wb_name = wb_id  # fallback
        if response_data.get("errors"):
            logger.error(response_data["errors"])
        elif response_data.get("data"):
            try:
                for workbook in response_data["data"]["workbooks"]:
                    wb_name = workbook["name"]
                    for view in workbook["views"]:
                        if view["luid"] == "":
                            hidden_views.append(view["name"])
            except Exception as e:
                print(e)

        if hidden_views is None:
            logger.info(f"Found no hidden views for workbook '{wb_name}'")
            return None

        logger.info(f"Found {len(hidden_views)} hidden views for workbook '{wb_name}'")
        return hidden_views
