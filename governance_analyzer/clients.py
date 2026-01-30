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


def get_account_client():
    """
    Get AccountClient with error handling.
    
    Uses credentials from configure_account_auth() or environment variables.
    Returns None if account credentials are not configured.
    
    Returns:
        AccountClient or None: Initialized account client or None if not configured
        
    Raises:
        ImportError: If Databricks SDK is not installed
    """
    try:
        from databricks.sdk import AccountClient
        
        # Check module config first, then environment variables
        account_id = _account_config["account_id"] or os.environ.get("DATABRICKS_ACCOUNT_ID")
        client_id = _account_config["client_id"] or os.environ.get("DATABRICKS_CLIENT_ID")
        client_secret = _account_config["client_secret"] or os.environ.get("DATABRICKS_CLIENT_SECRET")
        
        if not account_id:
            return None
        
        # Build AccountClient with explicit credentials if provided
        if client_id and client_secret:
            return AccountClient(
                host=f"https://accounts.cloud.databricks.com",
                account_id=account_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            # Fall back to default authentication (e.g., from environment or config file)
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
