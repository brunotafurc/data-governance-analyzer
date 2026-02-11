"""Governance check functions for Unity Catalog."""

from .clients import get_workspace_client, get_account_client, get_workspace_region, get_account_id


# =============================================================================
# Shared Constants and Utilities
# =============================================================================

SYSTEM_CATALOGS = {'system', '__databricks_internal', 'hive_metastore', 'samples'}
SYSTEM_MOUNT_POINTS = {'/', '/databricks-datasets', '/databricks-results', '/databricks', '/Volumes'}


def _iter_catalogs_schemas(w):
    """
    Iterate all non-system (catalog, schema) pairs.

    Skips system catalogs, foreign catalogs, and information_schema.
    Handles permission errors gracefully by skipping inaccessible objects.

    Args:
        w: WorkspaceClient instance

    Yields:
        tuple: (CatalogInfo, SchemaInfo)
    """
    try:
        catalogs = list(w.catalogs.list())
    except Exception as e:
        print(f"  [_iter_catalogs_schemas] Error listing catalogs: {e}")
        return

    for catalog in catalogs:
        cat_name = catalog.name
        if cat_name in SYSTEM_CATALOGS:
            continue
        cat_type = str(getattr(catalog, 'catalog_type', '') or '')
        if 'FOREIGN' in cat_type.upper():
            continue

        try:
            schemas = list(w.schemas.list(catalog_name=cat_name))
        except Exception as e:
            print(f"  [_iter_catalogs_schemas] Skipping catalog '{cat_name}': {e}")
            continue

        for schema in schemas:
            if schema.name == 'information_schema':
                continue
            yield catalog, schema


def _iter_schemas_tables(w, skip_views=True):
    """
    Iterate all tables across non-system catalogs and schemas.

    Args:
        w: WorkspaceClient instance
        skip_views: If True, skip VIEW table types

    Yields:
        tuple: (CatalogInfo, SchemaInfo, TableInfo)
    """
    for catalog, schema in _iter_catalogs_schemas(w):
        try:
            for table in w.tables.list(catalog_name=catalog.name, schema_name=schema.name):
                if skip_views:
                    table_type = str(getattr(table, 'table_type', '') or '').upper()
                    if table_type == 'VIEW':
                        continue
                yield catalog, schema, table
        except Exception as e:
            print(f"  [_iter_schemas_tables] Skipping schema '{catalog.name}.{schema.name}': {e}")
            continue


def _iter_schemas_volumes(w):
    """
    Iterate all volumes across non-system catalogs and schemas.

    Args:
        w: WorkspaceClient instance

    Yields:
        tuple: (CatalogInfo, SchemaInfo, VolumeInfo)
    """
    for catalog, schema in _iter_catalogs_schemas(w):
        try:
            for volume in w.volumes.list(catalog_name=catalog.name, schema_name=schema.name):
                yield catalog, schema, volume
        except Exception as e:
            print(f"  [_iter_schemas_volumes] Skipping schema '{catalog.name}.{schema.name}': {e}")
            continue


def _resolve_po_status(schema_po, catalog_po):
    """
    Resolve effective predictive optimization status via inheritance.

    PO follows inheritance: schema setting takes precedence over catalog.
    If both are INHERIT or None, PO is considered disabled.

    Args:
        schema_po: Schema's enable_predictive_optimization value
        catalog_po: Catalog's enable_predictive_optimization value

    Returns:
        bool: True if predictive optimization is effectively enabled
    """
    def _po_to_str(po_val):
        if po_val is None:
            return 'INHERIT'
        # Handle both 'ENABLE' and 'EnablePredictiveOptimization.ENABLE' formats
        return str(po_val).split('.')[-1].upper()

    schema_str = _po_to_str(schema_po)
    if schema_str == 'ENABLE':
        return True
    if schema_str == 'DISABLE':
        return False

    # Schema inherits -> check catalog
    catalog_str = _po_to_str(catalog_po)
    if catalog_str == 'ENABLE':
        return True
    return False


# =============================================================================
# Metastore Checks
# =============================================================================

def check_metastore_connected():
    """
    Check if UC Metastore is connected to workspace.
    
    Uses the Databricks SDK to verify that a Unity Catalog metastore
    is assigned to the current workspace.
    
    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_metastore_connected] Starting check...")
    
    try:
        print("[check_metastore_connected] Getting workspace client...")
        w = get_workspace_client()
        print("[check_metastore_connected] Workspace client obtained")
        
        # Get current metastore assignment for the workspace
        try:
            print("[check_metastore_connected] Fetching metastore summary...")
            metastore_summary = w.metastores.summary()
            print(f"[check_metastore_connected] Metastore summary received: {metastore_summary}")
            
            if metastore_summary and metastore_summary.metastore_id:
                metastore_name = metastore_summary.name or metastore_summary.metastore_id
                print(f"[check_metastore_connected] PASS - Metastore connected: {metastore_name}")
                return {
                    "status": "pass",
                    "score": 3,
                    "max_score": 3,
                    "details": f"Metastore '{metastore_name}' connected (ID: {metastore_summary.metastore_id})"
                }
            else:
                print("[check_metastore_connected] FAIL - No metastore connected")
                return {
                    "status": "fail",
                    "score": 0,
                    "max_score": 3,
                    "details": "No metastore connected to workspace"
                }
        except Exception as e:
            print(f"[check_metastore_connected] Exception: {e}")
            error_msg = str(e).lower()
            if "not found" in error_msg or "no metastore" in error_msg or "not assigned" in error_msg:
                return {
                    "status": "fail",
                    "score": 0,
                    "max_score": 3,
                    "details": "No metastore assigned to this workspace"
                }
            raise
            
    except ImportError as e:
        print(f"[check_metastore_connected] ImportError: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 3,
            "details": str(e)
        }
    except Exception as e:
        print(f"[check_metastore_connected] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 3,
            "details": f"Error checking metastore: {str(e)}"
        }


def check_metastore_region():
    """
    Check if workspace and metastore are in the same region.
    
    Compares the workspace region with the metastore region to ensure
    they are co-located for optimal performance and compliance.
    Supports AWS, Azure, and GCP Databricks deployments.
    
    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_metastore_region] Starting check...")
    
    try:
        print("[check_metastore_region] Getting workspace client...")
        w = get_workspace_client()
        print("[check_metastore_region] Workspace client obtained")
        
        # Get metastore details including region
        try:
            print("[check_metastore_region] Fetching metastore summary...")
            metastore_summary = w.metastores.summary()
            print(f"[check_metastore_region] Metastore summary: {metastore_summary}")
            
            if not metastore_summary or not metastore_summary.metastore_id:
                print("[check_metastore_region] No metastore connected")
                return {
                    "status": "fail",
                    "score": 0,
                    "max_score": 2,
                    "details": "No metastore connected - cannot check region"
                }
            
            # Get full metastore details to access region
            print(f"[check_metastore_region] Getting metastore details for ID: {metastore_summary.metastore_id}")
            metastore = w.metastores.get(id=metastore_summary.metastore_id)
            metastore_region = metastore.region if metastore else None
            print(f"[check_metastore_region] Metastore region: {metastore_region}")
            
            # Get workspace region using helper function
            print("[check_metastore_region] Detecting workspace region...")
            workspace_region = get_workspace_region(w)
            print(f"[check_metastore_region] Workspace region: {workspace_region}")
            
            if metastore_region:
                # Normalize region names for comparison
                metastore_region_normalized = metastore_region.lower().replace("_", "-")
                
                if workspace_region:
                    workspace_region_normalized = workspace_region.lower().replace("_", "-")
                    
                    # Check for exact match or if one contains the other
                    # (handles cases like "us-west-2" vs "uswest2")
                    metastore_clean = metastore_region_normalized.replace("-", "")
                    workspace_clean = workspace_region_normalized.replace("-", "")
                    
                    if (metastore_region_normalized == workspace_region_normalized or 
                        metastore_clean == workspace_clean):
                        print(f"[check_metastore_region] PASS - Regions match: {metastore_region}")
                        return {
                            "status": "pass",
                            "score": 2,
                            "max_score": 2,
                            "details": f"Metastore and workspace both in region: {metastore_region}"
                        }
                    else:
                        print(f"[check_metastore_region] FAIL - Region mismatch")
                        return {
                            "status": "fail",
                            "score": 0,
                            "max_score": 2,
                            "details": f"Region mismatch - Metastore: {metastore_region}, Workspace: {workspace_region}"
                        }
                else:
                    # Cannot determine workspace region, report metastore region
                    # This is still a pass since metastore is configured with a region
                    print(f"[check_metastore_region] PASS - Metastore region found, workspace region unknown")
                    return {
                        "status": "pass",
                        "score": 2,
                        "max_score": 2,
                        "details": f"Metastore region: {metastore_region} (workspace region could not be auto-detected, assumed same region)"
                    }
            else:
                print("[check_metastore_region] WARNING - Could not determine metastore region")
                return {
                    "status": "warning",
                    "score": 1,
                    "max_score": 2,
                    "details": "Could not determine metastore region"
                }
                
        except Exception as e:
            print(f"[check_metastore_region] Exception: {e}")
            error_msg = str(e).lower()
            if "not found" in error_msg or "no metastore" in error_msg:
                return {
                    "status": "fail",
                    "score": 0,
                    "max_score": 2,
                    "details": "No metastore assigned - cannot check region"
                }
            raise
            
    except ImportError as e:
        print(f"[check_metastore_region] ImportError: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": str(e)
        }
    except Exception as e:
        print(f"[check_metastore_region] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": f"Error checking metastore region: {str(e)}"
        }


# =============================================================================
# Identity and Access Checks
# =============================================================================

def check_scim_aim_provisioning():
    """
    Check if SCIM provisioning is enabled at the account level.
    
    Verifies that SCIM (System for Cross-domain Identity Management) is enabled
    in the account settings, which allows automated identity provisioning from 
    external IdPs like Azure AD, Okta, OneLogin to Databricks.
    
    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_scim_aim_provisioning] Starting check...")
    
    try:
        print("[check_scim_aim_provisioning] Getting account client...")
        account_client = get_account_client()
        
        if not account_client:
            print("[check_scim_aim_provisioning] No account client available")
            return {
                "status": "error",
                "score": 0,
                "max_score": 3,
                "details": "Cannot check SCIM provisioning: Account-level credentials not configured. Set DATABRICKS_ACCOUNT_HOST and DATABRICKS_ACCOUNT_ID environment variables."
            }
        
        print("[check_scim_aim_provisioning] Account client obtained, checking SCIM setting...")
        
        scim_enabled = False
        scim_status = "unknown"
        
        # Check SCIM provisioning setting at account level
        account_id = get_account_id()
        
        if account_id and hasattr(account_client, 'api_client'):
            try:
                # Check if SCIM provisioning is enabled via account settings API
                # The SCIM toggle is typically under identity/provisioning settings
                response = account_client.api_client.do(
                    'GET',
                    f'/api/2.0/accounts/{account_id}/scim/v2/ServiceProviderConfig'
                )
                if response:
                    # If we can access SCIM config, it means SCIM is enabled
                    scim_enabled = True
                    scim_status = "enabled"
                    print(f"[check_scim_aim_provisioning] SCIM ServiceProviderConfig accessible, SCIM is enabled")
            except Exception as e:
                error_msg = str(e).lower()
                print(f"[check_scim_aim_provisioning] SCIM ServiceProviderConfig check: {e}")
                
                # Try alternative endpoint for SCIM status
                try:
                    response = account_client.api_client.do(
                        'GET',
                        f'/api/2.0/accounts/{account_id}/settings/types/scim_provisioning/names/default'
                    )
                    if response:
                        scim_setting = response.get('scim_provisioning_setting', {})
                        scim_value = scim_setting.get('value', {})
                        scim_enabled = scim_value.get('enabled', False)
                        scim_status = "enabled" if scim_enabled else "disabled"
                        print(f"[check_scim_aim_provisioning] SCIM provisioning setting: {scim_status}")
                except Exception as e2:
                    print(f"[check_scim_aim_provisioning] Alternative SCIM check: {e2}")
        
        # If we still don't have a definitive answer, try checking if SCIM apps exist
        if scim_status == "unknown":
            try:
                # Check for SCIM applications/tokens at account level
                response = account_client.api_client.do(
                    'GET',
                    f'/api/2.0/accounts/{account_id}/scim/v2/Users',
                    {'count': 1}  # Just check if endpoint is accessible
                )
                if response is not None:
                    scim_enabled = True
                    scim_status = "enabled"
                    print("[check_scim_aim_provisioning] SCIM Users endpoint accessible, SCIM is enabled")
            except Exception as e:
                error_msg = str(e).lower()
                if "forbidden" in error_msg or "unauthorized" in error_msg:
                    # SCIM endpoint exists but we don't have permission - still means SCIM is configured
                    scim_enabled = True
                    scim_status = "enabled"
                    print("[check_scim_aim_provisioning] SCIM endpoint exists (permission denied), SCIM is enabled")
                elif "not found" in error_msg or "disabled" in error_msg:
                    scim_enabled = False
                    scim_status = "disabled"
                    print("[check_scim_aim_provisioning] SCIM endpoint not found/disabled")
                else:
                    print(f"[check_scim_aim_provisioning] SCIM Users check error: {e}")
        
        # Evaluate results
        print(f"[check_scim_aim_provisioning] Final status: scim_enabled={scim_enabled}, scim_status={scim_status}")
        
        if scim_enabled:
            print("[check_scim_aim_provisioning] PASS - SCIM provisioning is enabled")
            return {
                "status": "pass",
                "score": 3,
                "max_score": 3,
                "details": "SCIM provisioning is enabled at the account level"
            }
        elif scim_status == "disabled":
            print("[check_scim_aim_provisioning] FAIL - SCIM provisioning is disabled")
            return {
                "status": "fail",
                "score": 0,
                "max_score": 3,
                "details": "SCIM provisioning is disabled. Enable it in Account Console > Settings > User provisioning to sync identities from your Identity Provider (Azure AD, Okta, etc.)"
            }
        else:
            print("[check_scim_aim_provisioning] WARNING - Could not determine SCIM status")
            return {
                "status": "warning",
                "score": 1,
                "max_score": 3,
                "details": "Could not determine SCIM provisioning status. Verify in Account Console > Settings > User provisioning."
            }
            
    except ImportError as e:
        print(f"[check_scim_aim_provisioning] ImportError: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 3,
            "details": str(e)
        }
    except Exception as e:
        print(f"[check_scim_aim_provisioning] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 3,
            "details": f"Error checking SCIM provisioning: {str(e)}"
        }


def check_account_admin_group():
    """
    Check if Account Admin role is assigned to a group rather than individual users.
    
    Iterates through account groups to find those with the account_admin role.
    
    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_account_admin_group] Starting check...")
    
    try:
        print("[check_account_admin_group] Getting account client...")
        account_client = get_account_client()
        
        if not account_client:
            print("[check_account_admin_group] No account client available")
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": "Cannot check Account Admin assignments: Account-level credentials not configured."
            }
        
        print("[check_account_admin_group] Account client obtained")
        
        account_admin_groups = []
        
        # Iterate through groups to find those with account_admin role
        print("[check_account_admin_group] Iterating through groups...")
        try:
            for group in account_client.groups.list(attributes="id,displayName,roles"):
                roles = getattr(group, "roles", []) or []
                if any(getattr(r, "value", "") == "account_admin" for r in roles):
                    group_name = group.display_name or str(group.id)
                    account_admin_groups.append(group_name)
                    print(f"[check_account_admin_group] Found admin group: {group_name}")
        except Exception as e:
            print(f"[check_account_admin_group] Error iterating groups: {e}")
        
        print(f"[check_account_admin_group] Found {len(account_admin_groups)} admin group(s)")
        
        # Evaluate results - we only check if admin is assigned to groups
        if account_admin_groups:
            print("[check_account_admin_group] PASS - Admin assigned to groups")
            return {
                "status": "pass",
                "score": 2,
                "max_score": 2,
                "details": f"Account Admin role assigned to group(s): {', '.join(account_admin_groups)}"
            }
        else:
            print("[check_account_admin_group] FAIL - No admin groups found")
            return {
                "status": "fail",
                "score": 0,
                "max_score": 2,
                "details": "No groups with Account Admin role found. Create an admin group and assign the account_admin role to it."
            }
            
    except Exception as e:
        print(f"[check_account_admin_group] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": f"Error checking Account Admin group assignment: {str(e)}"
        }


def check_metastore_admin_group():
    """
    Check if Metastore Admin role is assigned to a group rather than individual users.
    
    Gets the owner directly from the metastore object and uses SCIM filters to 
    determine if the owner is a group, user, or service principal.
    
    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_metastore_admin_group] Starting check...")
    
    try:
        print("[check_metastore_admin_group] Getting workspace client...")
        w = get_workspace_client()
        print("[check_metastore_admin_group] Workspace client obtained successfully")
        
        # Get current metastore assignment for the workspace
        try:
            print("[check_metastore_admin_group] Fetching metastore summary...")
            metastore_summary = w.metastores.summary()
            print(f"[check_metastore_admin_group] Metastore summary: {metastore_summary}")
            
            if not metastore_summary or not metastore_summary.metastore_id:
                print("[check_metastore_admin_group] No metastore connected")
                return {
                    "status": "fail",
                    "score": 0,
                    "max_score": 2,
                    "details": "No metastore connected - cannot check metastore admin"
                }
            
            # Get owner directly from metastore - no need to iterate through groups
            metastore_owner = metastore_summary.owner
            metastore_name = metastore_summary.name or metastore_summary.metastore_id
            print(f"[check_metastore_admin_group] Metastore name: {metastore_name}, owner: {metastore_owner}")
            
            if not metastore_owner:
                print("[check_metastore_admin_group] Could not determine metastore owner")
                return {
                    "status": "warning",
                    "score": 1,
                    "max_score": 2,
                    "details": f"Could not determine metastore owner for '{metastore_name}'"
                }
            
            # Handle special system owner - this is fine
            if metastore_owner.lower() == "system user":
                print("[check_metastore_admin_group] Owner is 'System user' - system-managed metastore (OK)")
                return {
                    "status": "pass",
                    "score": 2,
                    "max_score": 2,
                    "details": f"Metastore '{metastore_name}' is managed by 'System user' (Databricks-managed)"
                }
            
            # Determine owner type using SCIM filters (single API calls, no iteration)
            is_group = False
            owner_type = "unknown"
            
            # Check if owner is a group using SCIM filter
            print(f"[check_metastore_admin_group] Checking if owner '{metastore_owner}' is a group...")
            try:
                for group in w.groups.list(filter=f'displayName eq "{metastore_owner}"'):
                    is_group = True
                    owner_type = "group"
                    print(f"[check_metastore_admin_group] Owner found as group")
                    break
            except Exception as e:
                print(f"[check_metastore_admin_group] Group lookup error: {e}")
            
            # If not a group, determine if it's a user or service principal
            if not is_group:
                # Users typically have email format with @
                if '@' in metastore_owner:
                    owner_type = "user"
                    print(f"[check_metastore_admin_group] Owner contains '@', classified as user")
                else:
                    # Check if it's a service principal using SCIM filter
                    print(f"[check_metastore_admin_group] Checking if owner is a service principal...")
                    try:
                        for sp in w.service_principals.list(filter=f'displayName eq "{metastore_owner}"'):
                            owner_type = "service_principal"
                            print(f"[check_metastore_admin_group] Owner found as service principal")
                            break
                    except Exception as e:
                        print(f"[check_metastore_admin_group] Service principal lookup error: {e}")
                    
                    # If still unknown, mark as unknown (don't assume group)
                    if owner_type == "unknown":
                        print(f"[check_metastore_admin_group] Could not determine owner type for '{metastore_owner}'")
            
            print(f"[check_metastore_admin_group] Final determination: is_group={is_group}, owner_type={owner_type}")
            
            # Evaluate results
            if is_group or owner_type == "group":
                print("[check_metastore_admin_group] PASS - Owner is a group")
                return {
                    "status": "pass",
                    "score": 2,
                    "max_score": 2,
                    "details": f"Metastore Admin for '{metastore_name}' is assigned to group: {metastore_owner}"
                }
            elif owner_type == "service_principal":
                print("[check_metastore_admin_group] WARNING - Owner is a service principal")
                return {
                    "status": "warning",
                    "score": 1,
                    "max_score": 2,
                    "details": f"Metastore Admin for '{metastore_name}' is assigned to service principal: {metastore_owner}. Consider assigning to a group for better manageability."
                }
            elif owner_type == "user":
                print("[check_metastore_admin_group] FAIL - Owner is an individual user")
                return {
                    "status": "fail",
                    "score": 0,
                    "max_score": 2,
                    "details": f"Metastore Admin for '{metastore_name}' is assigned to individual user: {metastore_owner}. Best practice is to assign to a group."
                }
            else:
                print("[check_metastore_admin_group] WARNING - Could not determine owner type")
                return {
                    "status": "warning",
                    "score": 1,
                    "max_score": 2,
                    "details": f"Could not determine owner type for '{metastore_owner}' on metastore '{metastore_name}'. Verify ownership in Unity Catalog settings."
                }
                
        except Exception as e:
            print(f"[check_metastore_admin_group] Exception during metastore check: {e}")
            error_msg = str(e).lower()
            if "not found" in error_msg or "no metastore" in error_msg:
                return {
                    "status": "fail",
                    "score": 0,
                    "max_score": 2,
                    "details": "No metastore assigned - cannot check metastore admin"
                }
            raise
            
    except ImportError as e:
        print(f"[check_metastore_admin_group] ImportError: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": str(e)
        }
    except Exception as e:
        print(f"[check_metastore_admin_group] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": f"Error checking Metastore Admin group assignment: {str(e)}"
        }


def check_workspace_admin_group():
    """
    Check if an account-level group is assigned as workspace admin.
    
    Verifies that the workspace 'admins' group contains at least one 
    account-level group as a member (best practice), rather than only
    individual users.
    
    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_workspace_admin_group] Starting check...")
    
    try:
        print("[check_workspace_admin_group] Getting workspace client...")
        w = get_workspace_client()
        print("[check_workspace_admin_group] Workspace client obtained")
        
        admin_groups = []
        admin_users = []
        
        # First, get all workspace groups to check membership against
        print("[check_workspace_admin_group] Getting list of workspace groups...")
        workspace_groups = set()
        try:
            for g in w.groups.list(attributes="displayName"):
                if g.display_name:
                    workspace_groups.add(g.display_name.lower())
        except Exception as e:
            print(f"[check_workspace_admin_group] Error getting workspace groups: {e}")
        
        print(f"[check_workspace_admin_group] Found {len(workspace_groups)} workspace groups")
        
        # Get the 'admins' group and check its members
        print("[check_workspace_admin_group] Getting 'admins' group members...")
        try:
            for group in w.groups.list(filter='displayName eq "admins"'):
                # Get the full group details to see members
                group_details = w.groups.get(id=group.id)
                members = getattr(group_details, "members", []) or []
                print(f"[check_workspace_admin_group] Found {len(members)} members in admins group")
                
                for member in members:
                    display = getattr(member, "display", None) or getattr(member, "value", None) or "unknown"
                    
                    # Check if this member name exists in the workspace groups
                    is_group = display.lower() in workspace_groups
                    
                    if is_group:
                        admin_groups.append(display)
                        print(f"[check_workspace_admin_group] Member '{display}' -> group")
                    else:
                        admin_users.append(display)
                        print(f"[check_workspace_admin_group] Member '{display}' -> user")
                break
        except Exception as e:
            print(f"[check_workspace_admin_group] Error checking admins group: {e}")
        
        print(f"[check_workspace_admin_group] Admin groups: {len(admin_groups)}, Admin users: {len(admin_users)}")
        
        # Evaluate results
        if admin_groups:
            print("[check_workspace_admin_group] PASS - Account-level group(s) assigned as admin")
            return {
                "status": "pass",
                "score": 2,
                "max_score": 2,
                "details": f"Workspace admin assigned to account-level group(s): {', '.join(admin_groups)}"
            }
        elif admin_users:
            print("[check_workspace_admin_group] FAIL - Only individual users are admins")
            return {
                "status": "fail",
                "score": 0,
                "max_score": 2,
                "details": f"Workspace admin assigned to {len(admin_users)} individual user(s) instead of groups. Assign an account-level group to the admins group."
            }
        else:
            print("[check_workspace_admin_group] WARNING - Could not determine admin assignments")
            return {
                "status": "warning",
                "score": 1,
                "max_score": 2,
                "details": "Could not determine workspace admin assignments. Verify that an account-level group is assigned as admin."
            }
            
    except Exception as e:
        print(f"[check_workspace_admin_group] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": f"Error checking Workspace Admin group: {str(e)}"
        }


def check_catalog_admin_group():
    """Check if Catalog Admin role is assigned to group."""
    return {"status": "pass", "score": 2, "max_score": 2, "details": "Assigned to group"}


def check_at_least_one_account_admin():
    """Check if at least 1 user is account admin."""
    return {"status": "pass", "score": 2, "max_score": 2, "details": "3 account admins"}


def check_account_admin_percentage():
    """Check if less than 5% of users are Account Admin."""
    return {"status": "pass", "score": 1, "max_score": 1, "details": "2% are admins"}


# =============================================================================
# Asset, Storage, and Compute Checks
# =============================================================================

def check_multiple_catalogs():
    """Check if multiple catalogs exist based on environment/BU/team."""
    return {"status": "pass", "score": 2, "max_score": 2, "details": "5 catalogs created"}


def check_catalog_binding():
    """Check if no catalog is bound to all workspaces."""
    return {"status": "pass", "score": 2, "max_score": 2, "details": "Limited binding"}


def check_managed_tables_percentage():
    """Check if managed tables/volumes > 70%."""
    return {"status": "pass", "score": 2, "max_score": 2, "details": "85% managed"}


def check_no_external_storage():
    """Check if no ADLS/S3 buckets used outside UC."""
    return {"status": "pass", "score": 3, "max_score": 3, "details": "All in UC"}


def check_uc_compute():
    """Check if compute is UC activated with right access mode."""
    return {"status": "pass", "score": 3, "max_score": 3, "details": "UC enabled"}


def check_no_hive_data():
    """Check if no data is in hive metastore."""
    return {"status": "pass", "score": 3, "max_score": 3, "details": "All migrated"}


def check_hive_disabled():
    """Check if hive metastore is disabled."""
    return {"status": "pass", "score": 0, "max_score": 0, "details": "Disabled"}


def check_system_tables():
    """Check if all system tables are activated (70%)."""
    return {"status": "pass", "score": 2, "max_score": 2, "details": "100% activated"}


def check_service_principals():
    """Check if production jobs use service principals."""
    return {"status": "pass", "score": 1, "max_score": 1, "details": "All jobs use SPs"}


def check_production_access():
    """Check if modify access to production is limited."""
    return {"status": "pass", "score": 1, "max_score": 1, "details": "Limited access"}


def check_group_ownership():
    """Check if 70% of assets have groups as owners."""
    return {"status": "pass", "score": 2, "max_score": 2, "details": "75% group owned"}


def check_predictive_optimization():
    """
    Check if 70% of managed tables have predictive optimization activated.

    Predictive Optimization is an AI-powered feature in Databricks that
    automatically manages and optimizes the layout and maintenance of
    Unity Catalog managed tables (OPTIMIZE, VACUUM, ANALYZE), improving
    query performance and reducing storage costs without manual intervention.

    PO status is resolved via inheritance: schema setting takes precedence,
    falling back to catalog setting. INHERIT or unset defaults to disabled.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_predictive_optimization] Starting check...")

    try:
        w = get_workspace_client()

        total_managed = 0
        po_enabled = 0

        print("[check_predictive_optimization] Iterating catalogs/schemas/tables...")
        for catalog, schema, table in _iter_schemas_tables(w, skip_views=True):
            table_type = str(getattr(table, 'table_type', '') or '').upper()
            if table_type != 'MANAGED':
                continue

            total_managed += 1

            # Resolve PO status via inheritance: schema -> catalog
            schema_po = getattr(schema, 'enable_predictive_optimization', None)
            catalog_po = getattr(catalog, 'enable_predictive_optimization', None)

            if _resolve_po_status(schema_po, catalog_po):
                po_enabled += 1

        print(f"[check_predictive_optimization] Total managed: {total_managed}, "
              f"PO enabled: {po_enabled}, PO disabled: {total_managed - po_enabled}")

        if total_managed == 0:
            return {
                "status": "warning",
                "score": 0,
                "max_score": 1,
                "details": "No managed tables found in Unity Catalog"
            }

        percentage = round(po_enabled / total_managed * 100, 1)

        if percentage >= 70:
            return {
                "status": "pass",
                "score": 1,
                "max_score": 1,
                "details": f"{percentage}% of managed tables ({po_enabled}/{total_managed}) "
                           f"have predictive optimization enabled"
            }
        else:
            return {
                "status": "fail",
                "score": 0,
                "max_score": 1,
                "details": f"Only {percentage}% of managed tables ({po_enabled}/{total_managed}) "
                           f"have predictive optimization enabled. Target: >= 70%"
            }

    except ImportError as e:
        return {"status": "error", "score": 0, "max_score": 1, "details": str(e)}
    except Exception as e:
        print(f"[check_predictive_optimization] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 1,
            "details": f"Error checking predictive optimization: {str(e)}"
        }


def check_no_dbfs_mounts():
    """
    Check if there are 0 mount storage accounts to DBFS.

    Data stored in DBFS mounts is considered legacy. All data should be
    managed through Unity Catalog for proper governance, security, and
    lineage tracking.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_no_dbfs_mounts] Starting check...")

    try:
        custom_mounts = []

        # Try using dbutils.fs.mounts() (available in Databricks notebook runtime)
        try:
            print("[check_no_dbfs_mounts] Attempting dbutils.fs.mounts()...")
            # dbutils is a global in Databricks notebook runtime
            all_mounts = dbutils.fs.mounts()  # noqa: F821

            for mount in all_mounts:
                mount_point = mount.mountPoint
                if mount_point not in SYSTEM_MOUNT_POINTS:
                    custom_mounts.append({
                        "mount_point": mount_point,
                        "source": mount.source
                    })
                    print(f"[check_no_dbfs_mounts] Found custom mount: {mount_point} -> {mount.source}")

            print(f"[check_no_dbfs_mounts] Found {len(custom_mounts)} custom mount(s)")

        except NameError:
            # dbutils not available - try SDK DBFS API as fallback
            print("[check_no_dbfs_mounts] dbutils not available, trying SDK DBFS API fallback...")
            try:
                w = get_workspace_client()
                mnt_entries = []
                try:
                    for entry in w.dbfs.list("/mnt"):
                        mnt_entries.append(entry)
                except Exception:
                    pass

                for entry in mnt_entries:
                    path = getattr(entry, 'path', '') or ''
                    if path and path not in SYSTEM_MOUNT_POINTS:
                        custom_mounts.append({
                            "mount_point": path,
                            "source": "unknown (detected via DBFS API)"
                        })

                print(f"[check_no_dbfs_mounts] Found {len(custom_mounts)} entry/entries under /mnt via DBFS API")
            except Exception as e:
                print(f"[check_no_dbfs_mounts] DBFS API fallback error: {e}")
                return {
                    "status": "error",
                    "score": 0,
                    "max_score": 3,
                    "details": f"Could not check DBFS mounts: dbutils unavailable and DBFS API failed: {str(e)}"
                }

        # Evaluate results
        if not custom_mounts:
            return {
                "status": "pass",
                "score": 3,
                "max_score": 3,
                "details": "No DBFS mount points found - all data is managed through Unity Catalog"
            }
        else:
            mount_list = [f"{m['mount_point']} -> {m['source']}" for m in custom_mounts[:10]]
            suffix = f" (and {len(custom_mounts) - 10} more)" if len(custom_mounts) > 10 else ""
            return {
                "status": "fail",
                "score": 0,
                "max_score": 3,
                "details": f"Found {len(custom_mounts)} DBFS mount(s) - legacy storage detected. "
                           f"Migrate data to Unity Catalog managed tables. Mounts: {'; '.join(mount_list)}{suffix}"
            }

    except ImportError as e:
        return {"status": "error", "score": 0, "max_score": 3, "details": str(e)}
    except Exception as e:
        print(f"[check_no_dbfs_mounts] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 3,
            "details": f"Error checking DBFS mounts: {str(e)}"
        }


def check_external_location_root():
    """
    Check if no external volumes or tables are created at the root of an
    external location.

    If you create external volumes or tables at the external location root,
    you cannot create any additional external volumes or tables on that
    external location. Instead, create them on a sub-directory inside the
    external location.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_external_location_root] Starting check...")

    try:
        w = get_workspace_client()

        # Step 1: List all external locations and collect their root URLs
        print("[check_external_location_root] Listing external locations...")
        ext_locations = []
        try:
            for loc in w.external_locations.list():
                ext_locations.append(loc)
        except Exception as e:
            print(f"[check_external_location_root] Error listing external locations: {e}")
            return {
                "status": "error",
                "score": 0,
                "max_score": 3,
                "details": f"Error listing external locations: {str(e)}"
            }

        if not ext_locations:
            return {
                "status": "pass",
                "score": 3,
                "max_score": 3,
                "details": "No external locations configured - nothing to validate"
            }

        # Build a set of normalized external location root URLs for fast lookup
        ext_url_map = {}  # normalized_url -> location_name
        for loc in ext_locations:
            if loc.url:
                normalized = loc.url.rstrip('/')
                ext_url_map[normalized] = loc.name or loc.url

        print(f"[check_external_location_root] Found {len(ext_url_map)} external location URL(s)")

        if not ext_url_map:
            return {
                "status": "pass",
                "score": 3,
                "max_score": 3,
                "details": "External locations found but none have URLs configured"
            }

        root_violations = []

        # Step 2: Check all external tables
        print("[check_external_location_root] Checking external tables...")
        for catalog, schema, table in _iter_schemas_tables(w, skip_views=True):
            table_type = str(getattr(table, 'table_type', '') or '').upper()
            if table_type != 'EXTERNAL':
                continue

            storage_loc = getattr(table, 'storage_location', None)
            if storage_loc:
                normalized_loc = storage_loc.rstrip('/')
                if normalized_loc in ext_url_map:
                    full_name = f"{catalog.name}.{schema.name}.{table.name}"
                    ext_loc_name = ext_url_map[normalized_loc]
                    root_violations.append(f"table '{full_name}' at root of '{ext_loc_name}'")
                    print(f"[check_external_location_root] VIOLATION: {full_name} at root of {ext_loc_name}")

        # Step 3: Check all external volumes
        print("[check_external_location_root] Checking external volumes...")
        for catalog, schema, volume in _iter_schemas_volumes(w):
            vol_type = str(getattr(volume, 'volume_type', '') or '').upper()
            if vol_type != 'EXTERNAL':
                continue

            storage_loc = getattr(volume, 'storage_location', None)
            if storage_loc:
                normalized_loc = storage_loc.rstrip('/')
                if normalized_loc in ext_url_map:
                    full_name = f"{catalog.name}.{schema.name}.{volume.name}"
                    ext_loc_name = ext_url_map[normalized_loc]
                    root_violations.append(f"volume '{full_name}' at root of '{ext_loc_name}'")
                    print(f"[check_external_location_root] VIOLATION: volume {full_name} at root of {ext_loc_name}")

        # Evaluate results
        print(f"[check_external_location_root] Found {len(root_violations)} violation(s)")

        if not root_violations:
            return {
                "status": "pass",
                "score": 3,
                "max_score": 3,
                "details": "No external tables or volumes are created at external location root paths"
            }
        else:
            details_list = '; '.join(root_violations[:5])
            suffix = f" (and {len(root_violations) - 5} more)" if len(root_violations) > 5 else ""
            return {
                "status": "fail",
                "score": 0,
                "max_score": 3,
                "details": f"Found {len(root_violations)} object(s) at external location root: "
                           f"{details_list}{suffix}. Move them to sub-directories."
            }

    except ImportError as e:
        return {"status": "error", "score": 0, "max_score": 3, "details": str(e)}
    except Exception as e:
        print(f"[check_external_location_root] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 3,
            "details": f"Error checking external location root: {str(e)}"
        }


def check_storage_credentials():
    """
    Check if independent storage credentials exist for each external location.

    Verifies that each external location uses a unique storage credential,
    ensuring access to different external locations is managed independently
    by different credentials.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_storage_credentials] Starting check...")

    try:
        w = get_workspace_client()

        print("[check_storage_credentials] Listing external locations...")
        ext_locations = []
        try:
            for loc in w.external_locations.list():
                ext_locations.append(loc)
        except Exception as e:
            print(f"[check_storage_credentials] Error listing external locations: {e}")
            return {
                "status": "error",
                "score": 0,
                "max_score": 1,
                "details": f"Error listing external locations: {str(e)}"
            }

        print(f"[check_storage_credentials] Found {len(ext_locations)} external location(s)")

        if not ext_locations:
            return {
                "status": "pass",
                "score": 1,
                "max_score": 1,
                "details": "No external locations configured - nothing to validate"
            }

        # Group external locations by their credential name
        credential_map = {}  # credential_name -> [location_names]
        for loc in ext_locations:
            cred = getattr(loc, 'credential_name', None) or 'unknown'
            loc_name = loc.name or loc.url or 'unnamed'
            if cred not in credential_map:
                credential_map[cred] = []
            credential_map[cred].append(loc_name)

        # Check for shared credentials (same credential used by multiple locations)
        shared_creds = {cred: locs for cred, locs in credential_map.items() if len(locs) > 1}

        print(f"[check_storage_credentials] Unique credentials: {len(credential_map)}, Shared: {len(shared_creds)}")

        if not shared_creds:
            return {
                "status": "pass",
                "score": 1,
                "max_score": 1,
                "details": f"All {len(ext_locations)} external location(s) use independent storage credentials"
            }
        else:
            shared_details = []
            for cred, locs in shared_creds.items():
                shared_details.append(f"'{cred}' shared by: {', '.join(locs)}")
            return {
                "status": "fail",
                "score": 0,
                "max_score": 1,
                "details": f"Shared credentials found - {'; '.join(shared_details)}. Each external location should have its own credential for independent access management."
            }

    except ImportError as e:
        return {"status": "error", "score": 0, "max_score": 1, "details": str(e)}
    except Exception as e:
        print(f"[check_storage_credentials] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 1,
            "details": f"Error checking storage credentials: {str(e)}"
        }


def check_data_quality():
    """
    Check if data quality monitoring is activated on 50% of tables.

    Data quality monitoring (Lakehouse Monitoring) helps ensure the quality
    of all data assets in Unity Catalog. It includes anomaly detection and
    data profiling capabilities, computing and monitoring data quality
    metrics over time.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_data_quality] Starting check...")

    try:
        w = get_workspace_client()

        total_tables = 0
        monitored_tables = 0
        monitor_errors = 0

        print("[check_data_quality] Iterating catalogs/schemas/tables...")
        for catalog, schema, table in _iter_schemas_tables(w, skip_views=True):
            table_type = str(getattr(table, 'table_type', '') or '').upper()
            if table_type not in ('MANAGED', 'EXTERNAL'):
                continue

            total_tables += 1
            full_name = f"{catalog.name}.{schema.name}.{table.name}"

            # Check if a Lakehouse Monitor exists for this table
            try:
                monitor = w.lakehouse_monitors.get(table_name=full_name)
                if monitor:
                    monitored_tables += 1
                    print(f"[check_data_quality] Monitor active: {full_name}")
            except Exception as e:
                error_msg = str(e).lower()
                if any(k in error_msg for k in ("not found", "does not exist",
                                                  "resource_does_not_exist",
                                                  "invalid_parameter_value")):
                    pass  # No monitor configured for this table - expected
                else:
                    monitor_errors += 1
                    if monitor_errors <= 3:
                        print(f"[check_data_quality] Monitor check error for {full_name}: {e}")

        print(f"[check_data_quality] Total tables: {total_tables}, "
              f"Monitored: {monitored_tables}, Check errors: {monitor_errors}")

        if total_tables == 0:
            return {
                "status": "warning",
                "score": 0,
                "max_score": 1,
                "details": "No tables found in Unity Catalog to check for data quality monitoring"
            }

        percentage = round(monitored_tables / total_tables * 100, 1)

        if percentage >= 50:
            return {
                "status": "pass",
                "score": 1,
                "max_score": 1,
                "details": f"{percentage}% of tables ({monitored_tables}/{total_tables}) "
                           f"have data quality monitoring enabled"
            }
        else:
            error_note = f" ({monitor_errors} tables had check errors)" if monitor_errors > 0 else ""
            return {
                "status": "fail",
                "score": 0,
                "max_score": 1,
                "details": f"Only {percentage}% of tables ({monitored_tables}/{total_tables}) "
                           f"have data quality monitoring enabled. Target: >= 50%{error_note}"
            }

    except ImportError as e:
        return {"status": "error", "score": 0, "max_score": 1, "details": str(e)}
    except Exception as e:
        print(f"[check_data_quality] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 1,
            "details": f"Error checking data quality monitoring: {str(e)}"
        }
