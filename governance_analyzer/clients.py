"""Client utilities for Databricks SDK connections.

All functions assume execution inside a Databricks environment.
"""

import os

# Module-level account configuration
_account_config = {
    "account_id": None,
    "client_id": None,
    "client_secret": None,
}


def configure_account_auth(account_id=None, client_id=None, client_secret=None):
    """
    Configure account-level authentication credentials.
    
    Call this before running checks that require account-level access.
    
    Args:
        account_id: Databricks account ID
        client_id: Service principal client ID
        client_secret: Service principal client secret
    """
    _account_config["account_id"] = account_id
    _account_config["client_id"] = client_id
    _account_config["client_secret"] = client_secret


def get_account_id():
    """
    Get the configured account ID.
    
    Returns:
        str or None: The account ID from config or environment
    """
    return _account_config["account_id"] or os.environ.get("DATABRICKS_ACCOUNT_ID")


def get_workspace_client():
    """
    Get WorkspaceClient with error handling.
    
    Returns:
        WorkspaceClient: Initialized Databricks workspace client
        
    Raises:
        ImportError: If Databricks SDK is not installed
    """
    try:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient()
    except ImportError:
        raise ImportError("Databricks SDK not available. Please install: pip install databricks-sdk")


def _detect_account_host():
    """
    Detect the correct account console URL based on the workspace cloud provider.

    Returns:
        str: The account console URL for the detected cloud (AWS, Azure, or GCP).
    """
    _ACCOUNT_HOSTS = {
        "azure": "https://accounts.azuredatabricks.net",
        "gcp": "https://accounts.gcp.databricks.com",
        "aws": "https://accounts.cloud.databricks.com",
    }
    try:
        from databricks.sdk import WorkspaceClient
        host = WorkspaceClient().config.host or ""
        if ".azuredatabricks.net" in host:
            return _ACCOUNT_HOSTS["azure"]
        if ".gcp.databricks.com" in host:
            return _ACCOUNT_HOSTS["gcp"]
    except Exception:
        pass
    return _ACCOUNT_HOSTS["aws"]


def get_account_client():
    """
    Get AccountClient with error handling.
    
    Uses credentials from configure_account_auth() or environment variables.
    Returns None if account credentials are not configured.
    Automatically detects the correct account host (AWS/Azure/GCP) from
    the workspace URL.
    
    Returns:
        AccountClient or None: Initialized account client or None if not configured
        
    Raises:
        ImportError: If Databricks SDK is not installed
    """
    try:
        from databricks.sdk import AccountClient
        
        account_id = _account_config["account_id"] or os.environ.get("DATABRICKS_ACCOUNT_ID")
        client_id = _account_config["client_id"] or os.environ.get("DATABRICKS_CLIENT_ID")
        client_secret = _account_config["client_secret"] or os.environ.get("DATABRICKS_CLIENT_SECRET")
        
        if not account_id:
            return None
        
        if client_id and client_secret:
            account_host = _detect_account_host()
            return AccountClient(
                host=account_host,
                account_id=account_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            return AccountClient(account_id=account_id)
            
    except ImportError:
        raise ImportError("Databricks SDK not available. Please install: pip install databricks-sdk")
    except Exception:
        return None


def get_workspace_region(workspace_client):
    """
    Get workspace region from Spark cluster tags.
    
    Args:
        workspace_client: WorkspaceClient instance (unused, kept for API compatibility)
        
    Returns:
        str or None: The workspace region
    """
    try:
        return spark.conf.get("spark.databricks.clusterUsageTags.region", None)
    except:
        return None
