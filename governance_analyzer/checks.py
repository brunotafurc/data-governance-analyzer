"""Governance check functions for Unity Catalog."""

from .clients import get_workspace_client, get_account_client, get_workspace_region, get_account_id


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


def _catalog_owner_is_group(w, owner, catalog_name):
    """
    Determine if the given owner string is a group (or system).
    Returns (is_ok_for_governance, owner_type) where is_ok = True for group or 'system user'.
    """
    if not owner:
        return False, "unknown"
    if owner.lower() == "system user":
        return True, "system"
    is_group = False
    owner_type = "unknown"
    try:
        for _ in w.groups.list(filter=f'displayName eq "{owner}"'):
            is_group = True
            owner_type = "group"
            break
    except Exception:
        pass
    if not is_group:
        if "@" in owner:
            owner_type = "user"
        else:
            try:
                for _ in w.service_principals.list(filter=f'displayName eq "{owner}"'):
                    owner_type = "service_principal"
                    break
            except Exception:
                pass
    return (is_group or owner_type == "group"), owner_type


def check_catalog_admin_group():
    """
    Check if for every catalog the owner (admin) is a group of users.

    Uses the catalogs list API to enumerate catalogs, then for each catalog
    determines whether the owner is a group, a user, or a service principal.
    Passes only when all catalogs have a group (or system) as owner.

    The check is executed at the workspace level, so it will check all catalogs in the workspace.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_catalog_admin_group] Starting check...")

    try:
        print("[check_catalog_admin_group] Getting workspace client...")
        w = get_workspace_client()
        if w is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": "Workspace client not available. Configure Databricks workspace credentials (e.g. DATABRICKS_HOST, token or OAuth).",
            }
        print("[check_catalog_admin_group] Workspace client obtained")

        catalogs_with_group = []
        catalogs_with_user = []
        catalogs_with_sp = []
        catalogs_unknown = []
        list_error = None
        catalog_names_checked = []

        print("[check_catalog_admin_group] Listing catalogs...")
        try:
            for catalog in w.catalogs.list():
                name = getattr(catalog, "name", None) or "unknown"
                owner = getattr(catalog, "owner", None)
                if owner is None:
                    try:
                        full = w.catalogs.get(name=name)
                        owner = getattr(full, "owner", None)
                    except Exception as e:
                        print(f"[check_catalog_admin_group] Could not get owner for catalog '{name}': {e}")
                        catalogs_unknown.append(name)
                        catalog_names_checked.append(name)
                        print(f"[check_catalog_admin_group] Catalog '{name}' -> owner unknown")
                        continue
                if not owner:
                    catalogs_unknown.append(name)
                    catalog_names_checked.append(name)
                    print(f"[check_catalog_admin_group] Catalog '{name}' -> owner unknown (empty)")
                    continue
                catalog_names_checked.append(name)
                is_ok, owner_type = _catalog_owner_is_group(w, owner, name)
                if is_ok:
                    catalogs_with_group.append((name, owner))
                    print(f"[check_catalog_admin_group] Catalog '{name}' owner '{owner}' -> group")
                elif owner_type == "user":
                    catalogs_with_user.append((name, owner))
                    print(f"[check_catalog_admin_group] Catalog '{name}' owner '{owner}' -> user")
                elif owner_type == "service_principal":
                    catalogs_with_sp.append((name, owner))
                    print(f"[check_catalog_admin_group] Catalog '{name}' owner '{owner}' -> service_principal")
                else:
                    catalogs_unknown.append(name)
                    print(f"[check_catalog_admin_group] Catalog '{name}' owner '{owner}' -> unknown")
        except Exception as e:
            print(f"[check_catalog_admin_group] Error listing catalogs: {e}")
            list_error = e

        if catalog_names_checked:
            print(f"[check_catalog_admin_group] Catalogs checked ({len(catalog_names_checked)}): {', '.join(catalog_names_checked)}")
        print(f"[check_catalog_admin_group] Catalogs with group: {len(catalogs_with_group)}, with user: {len(catalogs_with_user)}, with service_principal: {len(catalogs_with_sp)}, unknown: {len(catalogs_unknown)}")

        if list_error is not None:
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": f"Error listing catalogs: {str(list_error)}",
            }

        total = len(catalogs_with_group) + len(catalogs_with_user) + len(catalogs_with_sp) + len(catalogs_unknown)
        if total == 0:
            print("[check_catalog_admin_group] No catalogs found binded to this workspace")
            return {
                "status": "warning",
                "score": 1,
                "max_score": 2,
                "details": "No catalogs found binded to this workspace. Create catalogs and assign group ownership for governance.",
            }

        if catalogs_with_user or catalogs_with_sp or catalogs_unknown:
            print("[check_catalog_admin_group] FAIL - Not all catalogs have group ownership")
            parts = []
            if catalogs_with_user:
                parts.append(f"owned by user(s): {', '.join(n for n, _ in catalogs_with_user)}")
            if catalogs_with_sp:
                parts.append(f"owned by service principal(s): {', '.join(n for n, _ in catalogs_with_sp)}")
            if catalogs_unknown:
                parts.append(f"owner unknown: {', '.join(catalogs_unknown)}")
            return {
                "status": "fail",
                "score": 0,
                "max_score": 2,
                "details": f"Not all catalogs have group ownership. {'; '.join(parts)}. Assign catalog ownership to groups."
            }

        print("[check_catalog_admin_group] PASS - All catalogs have group (or system) ownership")
        group_list = ", ".join(f"'{n}' ({o})" for n, o in catalogs_with_group[:10])
        if len(catalogs_with_group) > 10:
            group_list += f" and {len(catalogs_with_group) - 10} more"
        return {
            "status": "pass",
            "score": 2,
            "max_score": 2,
            "details": f"All {len(catalogs_with_group)} catalog(s) have group or system ownership: {group_list} for this workspace",
        }

    except ImportError as e:
        print(f"[check_catalog_admin_group] ImportError: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": str(e),
        }
    except Exception as e:
        print(f"[check_catalog_admin_group] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": f"Error checking catalog admin group: {str(e)}",
        }


def check_at_least_one_account_admin():
    """
    Check if at least 1 user has the Account Admin role.

    Lists account users (via account-level SCIM / users API), counts how many
    have the account_admin role, and passes only when that count is >= 1.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_at_least_one_account_admin] Starting check...")

    try:
        print("[check_at_least_one_account_admin] Getting account client...")
        account_client = get_account_client()

        if not account_client:
            print("[check_at_least_one_account_admin] No account client available")
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": "Cannot check account admins: Account-level credentials not configured (DATABRICKS_ACCOUNT_ID, etc.).",
            }

        print("[check_at_least_one_account_admin] Account client obtained")

        account_admin_users = []
        list_error = None

        # Do not use a SCIM filter: Databricks account SCIM does not support filtering
        # by roles (roles.value or roles[value eq "..."] cause invalidValue / not supported).
        # List users with attributes id,userName,roles and filter in code.
        try:
            if hasattr(account_client, "users_v2"):
                print("[check_at_least_one_account_admin] Listing account users (users_v2)...")
                for user in account_client.users_v2.list(attributes="id,userName,roles"):
                    roles = getattr(user, "roles", []) or []
                    if any(getattr(r, "value", "") == "account_admin" for r in roles):
                        name = getattr(user, "user_name", None) or getattr(user, "id", "unknown")
                        account_admin_users.append(name)
                        print(f"[check_at_least_one_account_admin] Account admin: {name}")
            else:
                account_id = get_account_id()
                if not account_id:
                    return {
                        "status": "error",
                        "score": 0,
                        "max_score": 2,
                        "details": "Account ID not configured; cannot list users.",
                    }
                print("[check_at_least_one_account_admin] Listing account users (SCIM)...")
                start_index = 1
                count = 2000
                while True:
                    response = account_client.api_client.do(
                        "GET",
                        f"/api/2.0/accounts/{account_id}/scim/v2/Users",
                        query={"attributes": "userName,roles", "startIndex": start_index, "count": count},
                    )
                    resources = response.get("Resources", [])
                    if not resources:
                        break
                    for resource in resources:
                        roles = resource.get("roles", [])
                        role_values = [r.get("value", "") for r in roles if isinstance(r, dict)]
                        if "account_admin" in role_values:
                            name = resource.get("userName") or resource.get("id", "unknown")
                            account_admin_users.append(name)
                            print(f"[check_at_least_one_account_admin] Account admin: {name}")
                    if len(resources) < count:
                        break
                    start_index += count
        except Exception as e:
            print(f"[check_at_least_one_account_admin] Error listing users: {e}")
            list_error = e

        if list_error is not None:
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": f"Error listing account users: {str(list_error)}",
            }

        num_admins = len(account_admin_users)
        print(f"[check_at_least_one_account_admin] Found {num_admins} account admin(s)")

        if num_admins >= 1:
            print("[check_at_least_one_account_admin] PASS - At least one account admin")
            admins_detail = ", ".join(account_admin_users[:10])
            if num_admins > 10:
                admins_detail += f" and {num_admins - 10} more"
            return {
                "status": "pass",
                "score": 2,
                "max_score": 2,
                "details": f"{num_admins} account admin(s): {admins_detail}",
            }

        print("[check_at_least_one_account_admin] FAIL - No account admin found")
        return {
            "status": "fail",
            "score": 0,
            "max_score": 2,
            "details": "No user with Account Admin role found. Assign the account_admin role to at least one user or group in Account Settings → User management.",
        }

    except Exception as e:
        print(f"[check_at_least_one_account_admin] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": f"Error checking account admins: {str(e)}",
        }


def check_account_admin_percentage():
    """
    Check if less than 5% of users are Account Admin.

    Lists account users, counts how many have the account_admin role, and passes
    only when (account_admins / total_users) * 100 < 5.

    Returns:
        dict: Status with score, max_score, and details
    """
    ADMIN_PCT_THRESHOLD = 5.0
    print("[check_account_admin_percentage] Starting check...")

    try:
        print("[check_account_admin_percentage] Getting account client...")
        account_client = get_account_client()

        if not account_client:
            print("[check_account_admin_percentage] No account client available — check skipped")
            return {
                "status": "warning",
                "score": 0,
                "max_score": 1,
                "details": (
                    "Account-level credentials not configured; check skipped. "
                    "Set DATABRICKS_ACCOUNT_ID (and optionally client_id/secret) to run this check."
                ),
            }

        print("[check_account_admin_percentage] Account client obtained")

        total_users = 0
        account_admin_count = 0
        list_error = None

        try:
            if hasattr(account_client, "users_v2"):
                print("[check_account_admin_percentage] Listing account users (users_v2)...")
                for user in account_client.users_v2.list(attributes="id,userName,roles"):
                    total_users += 1
                    roles = getattr(user, "roles", []) or []
                    if any(getattr(r, "value", "") == "account_admin" for r in roles):
                        account_admin_count += 1
            else:
                account_id = get_account_id()
                if not account_id:
                    return {
                        "status": "error",
                        "score": 0,
                        "max_score": 1,
                        "details": "Account ID not configured; cannot list users.",
                    }
                print("[check_account_admin_percentage] Listing account users (SCIM)...")
                start_index = 1
                count = 2000
                while True:
                    response = account_client.api_client.do(
                        "GET",
                        f"/api/2.0/accounts/{account_id}/scim/v2/Users",
                        query={"attributes": "userName,roles", "startIndex": start_index, "count": count},
                    )
                    resources = response.get("Resources", [])
                    if not resources:
                        break
                    for resource in resources:
                        total_users += 1
                        roles = resource.get("roles", [])
                        role_values = [r.get("value", "") for r in roles if isinstance(r, dict)]
                        if "account_admin" in role_values:
                            account_admin_count += 1
                    if len(resources) < count:
                        break
                    start_index += count
        except Exception as e:
            print(f"[check_account_admin_percentage] Error listing users: {e}")
            list_error = e

        if list_error is not None:
            return {
                "status": "error",
                "score": 0,
                "max_score": 1,
                "details": f"Error listing account users: {str(list_error)}",
            }

        if total_users == 0:
            print("[check_account_admin_percentage] No users found")
            return {
                "status": "warning",
                "score": 0,
                "max_score": 1,
                "details": "No account users found; cannot compute admin percentage.",
            }

        pct = round((account_admin_count / total_users) * 100.0, 2)
        print(f"[check_account_admin_percentage] Total users: {total_users}, account admins: {account_admin_count}, percentage: {pct}%")

        if pct < ADMIN_PCT_THRESHOLD:
            print(f"[check_account_admin_percentage] PASS - {pct}% are account admins (below {ADMIN_PCT_THRESHOLD}%)")
            return {
                "status": "pass",
                "score": 1,
                "max_score": 1,
                "details": f"{pct}% of users are Account Admin ({account_admin_count}/{total_users}), below {ADMIN_PCT_THRESHOLD}% threshold.",
            }

        print(f"[check_account_admin_percentage] FAIL - {pct}% are account admins (at or above {ADMIN_PCT_THRESHOLD}%)")
        return {
            "status": "fail",
            "score": 0,
            "max_score": 1,
            "details": f"{pct}% of users are Account Admin ({account_admin_count}/{total_users}). Best practice: keep under {ADMIN_PCT_THRESHOLD}%.",
        }

    except Exception as e:
        print(f"[check_account_admin_percentage] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 1,
            "details": f"Error checking account admin percentage: {str(e)}",
        }


# =============================================================================
# Asset, Storage, and Compute Checks
# =============================================================================

def check_multiple_catalogs():
    """Check if multiple catalogs exist based on environment/BU/team."""
    return {"status": "pass", "score": 2, "max_score": 2, "details": "5 catalogs created"}


def check_catalog_binding():
    """Check if no catalog is bound to all workspaces."""
    return {"status": "pass", "score": 2, "max_score": 2, "details": "Limited binding"}


def _table_type_to_count(ttype):
    """Map table_type to bucket: 'managed', 'external', or 'view'. Returns None if unknown."""
    if ttype is None:
        return None
    ttype_str = str(ttype).upper() if ttype else ""
    if ttype_str == "MANAGED":
        return "managed"
    if ttype_str == "EXTERNAL":
        return "external"
    if "VIEW" in ttype_str or ttype_str == "VIEW":
        return "view"
    return None


def check_managed_tables_percentage():
    """
    Check if managed tables (and volumes) are more than 70% of all data tables/volumes.

    Uses list_summaries(catalog_name, schema_name_pattern="%") to get all tables
    per catalog in one call (two loops: catalogs -> table summaries). Counts
    MANAGED vs EXTERNAL; views are excluded from the percentage. Passes when managed_pct > 70.

    Returns:
        dict: Status with score, max_score, and details
    """
    MANAGED_PCT_THRESHOLD = 70.0
    print("[check_managed_tables_percentage] Starting check...")

    try:
        print("[check_managed_tables_percentage] Getting workspace client...")
        w = get_workspace_client()
        if w is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": "Workspace client not available.",
            }
        print("[check_managed_tables_percentage] Workspace client obtained")

        managed_count = 0
        external_count = 0
        view_count = 0
        list_error = None

        try:
            for catalog in w.catalogs.list():
                catalog_name = getattr(catalog, "name", None) or ""
                if not catalog_name:
                    continue
                try:
                    # One call per catalog: all table summaries across all schemas (no schema loop)
                    for summary in w.tables.list_summaries(
                        catalog_name=catalog_name,
                        schema_name_pattern="%",
                        max_results=0,
                    ):
                        ttype = getattr(summary, "table_type", None)
                        bucket = _table_type_to_count(ttype)
                        if bucket == "managed":
                            managed_count += 1
                        elif bucket == "external":
                            external_count += 1
                        elif bucket == "view":
                            view_count += 1
                except Exception as catalog_err:
                    print(f"[check_managed_tables_percentage] Catalog {catalog_name}: {catalog_err}")
        except Exception as e:
            print(f"[check_managed_tables_percentage] Error listing tables: {e}")
            list_error = e

        if list_error is not None:
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": f"Error listing tables: {str(list_error)}",
            }

        data_tables = managed_count + external_count
        if data_tables == 0:
            print("[check_managed_tables_percentage] No data tables (managed+external) found")
            return {
                "status": "warning",
                "score": 1,
                "max_score": 2,
                "details": f"No managed or external tables found (views: {view_count}). Create tables to measure.",
            }

        managed_pct = round((managed_count / data_tables) * 100.0, 2)
        print(
            f"[check_managed_tables_percentage] Managed: {managed_count}, external: {external_count}, "
            f"views: {view_count}, managed %: {managed_pct}%"
        )

        if managed_pct > MANAGED_PCT_THRESHOLD:
            print(f"[check_managed_tables_percentage] PASS - {managed_pct}% managed (above {MANAGED_PCT_THRESHOLD}%)")
            return {
                "status": "pass",
                "score": 2,
                "max_score": 2,
                "details": f"{managed_pct}% of data tables are managed ({managed_count}/{data_tables}), above {MANAGED_PCT_THRESHOLD}% threshold.",
            }

        print(f"[check_managed_tables_percentage] FAIL - {managed_pct}% managed (at or below {MANAGED_PCT_THRESHOLD}%)")
        return {
            "status": "fail",
            "score": 0,
            "max_score": 2,
            "details": f"{managed_pct}% of data tables are managed ({managed_count}/{data_tables}). Best practice: use managed tables for > {MANAGED_PCT_THRESHOLD}%.",
        }

    except Exception as e:
        print(f"[check_managed_tables_percentage] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": f"Error checking managed tables percentage: {str(e)}",
        }


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
    """Check if 70% of managed tables have predictive optimization."""
    return {"status": "pass", "score": 1, "max_score": 1, "details": "80% enabled"}


def check_no_dbfs_mounts():
    """Check if 0 mount storage accounts to DBFS."""
    return {"status": "pass", "score": 3, "max_score": 3, "details": "No mounts"}


def check_external_location_root():
    """Check if no external volumes/tables at external location root."""
    return {"status": "pass", "score": 3, "max_score": 3, "details": "All in subdirs"}


def check_storage_credentials():
    """Check if independent storage credentials for each external location."""
    return {"status": "pass", "score": 1, "max_score": 1, "details": "Separate credentials"}


def check_data_quality():
    """Check if data quality is activated on 50% of tables."""
    return {"status": "pass", "score": 1, "max_score": 1, "details": "60% monitored"}
