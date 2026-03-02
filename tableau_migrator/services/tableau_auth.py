import logging
import tableauserverclient as TSC

from typing import Optional, Tuple


logger = logging.getLogger(__name__)


def tableau_authenticate(
    token_name: str, token_value: str, portal_url: str, site_id: Optional[str] = None
) -> Tuple[TSC.Server, TSC.PersonalAccessTokenAuth]:
    """Authenticate with Tableau Server.

    Args:
        token_name (str): The Personal Access Token name
        token_value (str): The Personal Access Token secret value
        portal_url (str): The Tableau Server URL
        site_id (str, optional): The Tableau Server site URI. Leave blank if site
        name/URI is 'Default'

    Returns :
        server (TSC.Server): a TableauServerClient variable containing server
            connection details
        tableau_auth (TSC.PersonalAccessTokenAuth) : a PersonalAccessTokenAuth object
            used for server authentication
    """
    server = TSC.Server(portal_url)
    # if using SSL uncomment below
    # server.add_http_options({'verify':True, 'cert': ssl_chain_cert})
    # to bypass SSL, use below
    server.add_http_options({"verify": False})
    server.use_server_version()
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_id=site_id)
    return server, tableau_auth
