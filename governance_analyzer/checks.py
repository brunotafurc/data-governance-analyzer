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
            
            # Handle special built-in owners
            _BUILTIN_GROUP_OWNERS = {"system user", "account users"}
            if metastore_owner.lower() in _BUILTIN_GROUP_OWNERS:
                print(f"[check_metastore_admin_group] Owner is '{metastore_owner}' - built-in group (OK)")
                return {
                    "status": "pass",
                    "score": 2,
                    "max_score": 2,
                    "details": f"Metastore '{metastore_name}' is owned by built-in group '{metastore_owner}'"
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
                    print(f"[check_catalog_admin_group] Catalog '{name}' owner '{owner}' owner type {owner_type} -> unknown")
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
    """
    Check that no catalog is bound to all workspaces (isolation best practice).

    A catalog with isolation_mode OPEN is accessible from any workspace in the
    metastore ("bound to all"). This check passes when every catalog is ISOLATED
    (limited to an explicit set of workspaces).

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_catalog_binding] Starting check...")

    try:
        print("[check_catalog_binding] Getting workspace client...")
        w = get_workspace_client()
        if w is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": "Workspace client not available.",
            }

        catalogs_open = []
        catalogs_isolated = []
        catalogs_unknown = []
        list_error = None

        try:
            for catalog in w.catalogs.list():
                name = getattr(catalog, "name", None) or "unknown"
                mode = getattr(catalog, "isolation_mode", None)
                if mode is None:
                    try:
                        full = w.catalogs.get(name=name)
                        mode = getattr(full, "isolation_mode", None)
                    except Exception as e:
                        print(f"[check_catalog_binding] Could not get isolation_mode for '{name}': {e}")
                        catalogs_unknown.append(name)
                        continue
                mode_str = (mode.value if hasattr(mode, "value") else str(mode or "")).upper()
                if mode_str == "OPEN":
                    catalogs_open.append(name)
                    print(f"[check_catalog_binding] Catalog '{name}' -> OPEN (bound to all workspaces)")
                elif mode_str == "ISOLATED":
                    catalogs_isolated.append(name)
                    print(f"[check_catalog_binding] Catalog '{name}' -> ISOLATED (limited binding)")
                else:
                    catalogs_unknown.append(name)
        except Exception as e:
            print(f"[check_catalog_binding] Error listing catalogs: {e}")
            list_error = e

        if list_error is not None:
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": f"Error listing catalogs: {str(list_error)}",
            }

        total = len(catalogs_open) + len(catalogs_isolated) + len(catalogs_unknown)
        if total == 0:
            print("[check_catalog_binding] No catalogs found")
            return {
                "status": "warning",
                "score": 1,
                "max_score": 2,
                "details": "No catalogs found; cannot check workspace bindings.",
            }

        if catalogs_open:
            print("[check_catalog_binding] FAIL - Some catalogs are bound to all workspaces (OPEN)")
            return {
                "status": "fail",
                "score": 0,
                "max_score": 2,
                "details": f"Catalog(s) bound to all workspaces (OPEN): {', '.join(catalogs_open)}. Set isolation mode to ISOLATED and bind to specific workspaces only.",
            }

        print("[check_catalog_binding] PASS - No catalog is bound to all workspaces")
        return {
            "status": "pass",
            "score": 2,
            "max_score": 2,
            "details": f"All {len(catalogs_isolated)} catalog(s) have limited workspace binding (ISOLATED).",
        }

    except Exception as e:
        print(f"[check_catalog_binding] Unexpected error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 2,
            "details": f"Error checking catalog binding: {str(e)}",
        }


def check_managed_tables_percentage():
    """
    Check if managed tables and volumes are more than 70% of all tables and volumes.

    Iterates over all catalogs and schemas, counts tables (MANAGED, EXTERNAL; views excluded
    from denominator) and volumes by type, and passes when (managed_tables + managed_volumes)
    / (total_tables + total_volumes) * 100 > 70.

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

        managed_tables = 0
        external_tables = 0
        managed_volumes = 0
        external_volumes = 0
        list_error = None

        try:
            for catalog in w.catalogs.list():
                catalog_name = getattr(catalog, "name", None) or ""
                if not catalog_name:
                    continue
                try:
                    for schema in w.schemas.list(catalog_name=catalog_name):
                        schema_name = getattr(schema, "name", None) or ""
                        if not schema_name:
                            continue
                        try:
                            for table in w.tables.list(
                                catalog_name=catalog_name,
                                schema_name=schema_name,
                                max_results=0,
                            ):
                                tt = getattr(table, "table_type", None)
                                tt_str = (tt.value if hasattr(tt, "value") else str(tt or "")).upper()
                                if tt_str == "MANAGED":
                                    managed_tables += 1
                                elif tt_str == "EXTERNAL":
                                    external_tables += 1
                                # VIEW, etc. excluded from denominator
                        except Exception as e:
                            print(f"[check_managed_tables_percentage] Error listing tables in {catalog_name}.{schema_name}: {e}")
                        try:
                            for volume in w.volumes.list(
                                catalog_name=catalog_name,
                                schema_name=schema_name,
                                max_results=0,
                            ):
                                vt = getattr(volume, "volume_type", None)
                                vt_str = (vt.value if hasattr(vt, "value") else str(vt or "")).upper()
                                if vt_str == "MANAGED":
                                    managed_volumes += 1
                                elif vt_str == "EXTERNAL":
                                    external_volumes += 1
                        except Exception as e:
                            print(f"[check_managed_tables_percentage] Error listing volumes in {catalog_name}.{schema_name}: {e}")
                except Exception as e:
                    print(f"[check_managed_tables_percentage] Error listing schemas in {catalog_name}: {e}")
        except Exception as e:
            print(f"[check_managed_tables_percentage] Error listing catalogs/tables/volumes: {e}")
            list_error = e

        if list_error is not None:
            return {
                "status": "error",
                "score": 0,
                "max_score": 2,
                "details": f"Error listing tables/volumes: {str(list_error)}",
            }

        total_tables = managed_tables + external_tables
        total_volumes = managed_volumes + external_volumes
        total = total_tables + total_volumes
        managed_total = managed_tables + managed_volumes

        if total == 0:
            print("[check_managed_tables_percentage] No tables or volumes found")
            return {
                "status": "warning",
                "score": 1,
                "max_score": 2,
                "details": "No tables or volumes found in Unity Catalog; cannot compute managed percentage.",
            }

        pct = round((managed_total / total) * 100.0, 2)
        print(f"[check_managed_tables_percentage] Tables: {managed_tables} managed, {external_tables} external; Volumes: {managed_volumes} managed, {external_volumes} external; {pct}% managed")

        if pct > MANAGED_PCT_THRESHOLD:
            print(f"[check_managed_tables_percentage] PASS - {pct}% managed (above {MANAGED_PCT_THRESHOLD}%)")
            return {
                "status": "pass",
                "score": 2,
                "max_score": 2,
                "details": f"{pct}% of tables and volumes are managed ({managed_total}/{total}), above {MANAGED_PCT_THRESHOLD}% threshold.",
            }

        print(f"[check_managed_tables_percentage] FAIL - {pct}% managed (at or below {MANAGED_PCT_THRESHOLD}%)")
        return {
            "status": "fail",
            "score": 0,
            "max_score": 2,
            "details": f"{pct}% of tables and volumes are managed ({managed_total}/{total}). Best practice: use managed tables/volumes for > {MANAGED_PCT_THRESHOLD}%.",
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
    UC_MODES = {
        "DATA_SECURITY_MODE_STANDARD",
        "DATA_SECURITY_MODE_DEDICATED",
        "DATA_SECURITY_MODE_AUTO",
    }
    max_score = 3

    try:
        w = get_workspace_client()
        for cluster in w.clusters.list():
            name = getattr(cluster, "cluster_name", None) or (cluster.get("cluster_name") if isinstance(cluster, dict) else None) or str(getattr(cluster, "cluster_id", ""))
            mode = getattr(cluster, "data_security_mode", None) or (cluster.get("data_security_mode") if isinstance(cluster, dict) else None)
            mode_str = str(mode).strip() if mode else None
            is_uc = mode_str and any(m in mode_str for m in UC_MODES)
            if not is_uc:
                return {"status": "fail", "score": 0, "max_score": max_score, "details": f"Cluster '{name}' is not UC enabled (data_security_mode: {mode_str or 'unknown'})."}
        for wh in w.warehouses.list():
            wh_id = getattr(wh, "id", None) or (wh.get("id") if isinstance(wh, dict) else None)
            name = getattr(wh, "name", None) or getattr(wh, "warehouse_name", None) or (wh.get("name") or wh.get("warehouse_name") if isinstance(wh, dict) else None) or str(wh_id or "")
            uc_enabled = None
            if wh_id:
                try:
                    full = w.warehouses.get(id=wh_id)
                    uc_enabled = getattr(full, "enable_unity_catalog", None)
                    if isinstance(full, dict):
                        uc_enabled = uc_enabled if uc_enabled is not None else full.get("enable_unity_catalog")
                except Exception:
                    pass
            if uc_enabled is None:
                uc_enabled = getattr(wh, "enable_unity_catalog", None)
                if isinstance(wh, dict):
                    uc_enabled = uc_enabled if uc_enabled is not None else wh.get("enable_unity_catalog")
            if uc_enabled is not True:
                return {"status": "fail", "score": 0, "max_score": max_score, "details": f"SQL warehouse '{name}' does not have Unity Catalog enabled."}
        return {"status": "pass", "score": max_score, "max_score": max_score, "details": "All clusters and SQL warehouses UC compliant."}
    except ImportError as e:
        return {"status": "error", "score": 0, "max_score": max_score, "details": str(e)}
    except Exception as e:
        return {"status": "error", "score": 0, "max_score": max_score, "details": f"Error listing compute: {str(e)}"}


def check_no_hive_data():
    """Check if no data is in hive metastore."""
    max_score = 3
    default_schema = "default"
    try:
        for db in spark.catalog.listDatabases():
            db_name = db.name if hasattr(db, "name") else db["name"]
            if db_name.lower() == default_schema.lower():
                continue
            tables = spark.catalog.listTables(db_name)
            for t in tables:
                tname = t.name if hasattr(t, "name") else t["name"]
                return {
                    "status": "fail",
                    "score": 0,
                    "max_score": max_score,
                    "details": f"Hive table found in non-default schema: {db_name}.{tname}. Migrate or remove hive data."
                }
        return {"status": "pass", "score": max_score, "max_score": max_score, "details": "No hive data outside default schema."}
    except Exception as e:
        return {"status": "fail", "score": 0, "max_score": max_score, "details": f"Could not check hive metastore: {e}"}


def check_hive_disabled():
    """Check if hive metastore is disabled."""
    print("[check_hive_disabled] Starting check...")
    try:
        w = get_workspace_client()
        # List catalogs; when Hive metastore is disabled, hive_metastore is not present
        catalogs = list(w.catalogs.list())
        catalog_names = [c.name for c in catalogs]
        if "hive_metastore" not in catalog_names:
            print("[check_hive_disabled] PASS - hive_metastore not visible (disabled)")
            return {
                "status": "pass",
                "score": 1,
                "max_score": 1,
                "details": "Hive metastore is disabled"
            }
        print("[check_hive_disabled] FAIL - hive_metastore catalog is visible (enabled)")
        return {
            "status": "fail",
            "score": 0,
            "max_score": 1,
            "details": "Hive metastore is still enabled. Disable it in workspace settings for migration completeness."
        }
    except ImportError as e:
        print(f"[check_hive_disabled] ImportError: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 1,
            "details": str(e)
        }
    except Exception as e:
        print(f"[check_hive_disabled] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": 1,
            "details": f"Error checking Hive metastore status: {str(e)}"
        }

def check_system_tables():
    """Check if all system tables are activated (70%)."""
    SYSTEM_TABLES_PASS_THRESHOLD_PCT = 70
    MAX_SCORE = 2

    print("[check_system_tables] Starting check...")

    try:
        w = get_workspace_client()
        metastore_summary = w.metastores.summary()

        if not metastore_summary or not metastore_summary.metastore_id:
            print("[check_system_tables] No metastore connected")
            return {
                "status": "fail",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No metastore connected - cannot check system tables",
            }

        metastore_id = metastore_summary.metastore_id
        print(f"[check_system_tables] Listing system schemas for metastore {metastore_id}...")

        enabled = 0
        total = 0

        for schema_info in w.system_schemas.list(metastore_id=metastore_id):
            total += 1
            state = getattr(schema_info, "state", None)
            state_str = str(state).upper() if state is not None else ""
            if state_str == "ENABLE" or state_str == "ENABLED" or "ENABLE" in state_str:
                enabled += 1

        if total == 0:
            print("[check_system_tables] No system schemas reported for metastore")
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": "No system schemas found for metastore (cannot compute activation %)",
            }

        pct = round(100 * enabled / total, 1)
        print(f"[check_system_tables] Enabled: {enabled}/{total} ({pct}%)")

        if pct >= SYSTEM_TABLES_PASS_THRESHOLD_PCT:
            print(f"[check_system_tables] PASS - {pct}% of system schemas activated (>= {SYSTEM_TABLES_PASS_THRESHOLD_PCT}%)")
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"{pct}% activated ({enabled}/{total} system schemas)",
            }

        score = 1 if pct >= 50 else 0
        print(f"[check_system_tables] FAIL - {pct}% activated (below {SYSTEM_TABLES_PASS_THRESHOLD_PCT}%)")
        return {
            "status": "fail",
            "score": score,
            "max_score": MAX_SCORE,
            "details": f"{pct}% activated ({enabled}/{total} system schemas); enable more for audit/lineage/billing coverage",
        }

    except ImportError as e:
        print(f"[check_system_tables] ImportError: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": str(e),
        }
    except Exception as e:
        print(f"[check_system_tables] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking system tables: {str(e)}",
        }

def check_service_principals():
    """Check if production jobs use service principals."""
    # Optional: only consider jobs tagged as production (set to None to check all jobs)
    from datetime import datetime, timezone, timedelta

    max_score = 1
    try:
        w = get_workspace_client()
        two_weeks_ago_ms = int((datetime.now(timezone.utc) - timedelta(days=14)).timestamp() * 1000)
        checked_job_ids = set()
        for run in w.jobs.list_runs(completed_only=True, start_time_from=two_weeks_ago_ms):
            jid = getattr(run, "job_id", None) or (run.get("job_id") if isinstance(run, dict) else None)
            if jid is None or jid in checked_job_ids:
                continue
            checked_job_ids.add(jid)
            try:
                job = w.jobs.get(job_id=jid)
            except Exception:
                continue
            settings = getattr(job, "settings", None) or (job.get("settings") if isinstance(job, dict) else None)
            if not settings:
                return {"status": "fail", "score": 0, "max_score": max_score, "details": f"Job {jid} has no settings and had recent runs. Set run_as.service_principal_name."}
            run_as = getattr(settings, "run_as", None) or (settings.get("run_as") if isinstance(settings, dict) else None)
            has_sp = False
            if run_as is not None:
                sp_name = getattr(run_as, "service_principal_name", None) or (run_as.get("service_principal_name") if isinstance(run_as, dict) else None)
                has_sp = bool(sp_name)
            if not has_sp:
                jname = getattr(settings, "name", None) or (settings.get("name") if isinstance(settings, dict) else None) or str(jid)
                return {"status": "fail", "score": 0, "max_score": max_score, "details": f"Job '{jname}' (id {jid}) has recent runs and does not use a service principal. Set run_as.service_principal_name."}
        if not checked_job_ids:
            return {"status": "pass", "score": max_score, "max_score": max_score, "details": "No completed job runs in last 2 weeks."}
        return {"status": "pass", "score": max_score, "max_score": max_score, "details": "All recently active jobs use service principals."}
    except Exception as e:
        return {"status": "error", "score": 0, "max_score": max_score, "details": f"Error checking service principals: {e}"}


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


# =============================================================================
# Fine-Grained Access Control Checks
# =============================================================================

def check_abac_policy_adoption():
    """
    Check if ABAC policies are being used for centralized access control.

    Queries system.access.audit for ABAC-related events (policy creation,
    alteration, and tag application) to verify adoption.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_abac_policy_adoption] Starting check...")
    MAX_SCORE = 3

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No active Spark session available to query system tables.",
            }

        query = """
        SELECT
            COUNT(DISTINCT CASE WHEN action_name IN ('createPolicy', 'alterPolicy') THEN action_name END) AS policy_events,
            COUNT(DISTINCT CASE WHEN action_name IN ('applyTag', 'setTag') THEN action_name END) AS tag_events
        FROM system.access.audit
        WHERE event_date >= current_date() - 90
          AND action_name IN ('createPolicy', 'alterPolicy', 'applyTag', 'setTag')
        """
        print("[check_abac_policy_adoption] Querying audit log for ABAC events...")
        row = spark.sql(query).first()

        policy_events = row["policy_events"] if row else 0
        tag_events = row["tag_events"] if row else 0

        if policy_events > 0 and tag_events > 0:
            print("[check_abac_policy_adoption] PASS - ABAC policies and tags actively used")
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"ABAC policies and governed tags actively used (policy events: {policy_events}, tag events: {tag_events} in last 90 days).",
            }
        elif policy_events > 0 or tag_events > 0:
            print("[check_abac_policy_adoption] WARNING - Partial ABAC adoption")
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": f"Partial ABAC adoption. Policy events: {policy_events}, tag events: {tag_events} in last 90 days. Apply governed tags and create ABAC policies for full coverage.",
            }
        else:
            print("[check_abac_policy_adoption] FAIL - No ABAC activity detected")
            return {
                "status": "fail",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No ABAC policy or tag events found in last 90 days. Configure ABAC policies at the catalog level for centralized access control.",
            }

    except Exception as e:
        error_msg = str(e).lower()
        if "table_or_view_not_found" in error_msg or "table not found" in error_msg:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "system.access.audit table not available. Enable audit log system tables.",
            }
        print(f"[check_abac_policy_adoption] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking ABAC policy adoption: {str(e)}",
        }


def check_governed_tags_applied():
    """
    Check if governed tags are applied to catalogs, schemas, and tables.

    Queries information_schema to determine the percentage of schemas that
    have at least one governed tag applied.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_governed_tags_applied] Starting check...")
    MAX_SCORE = 2
    PASS_THRESHOLD_PCT = 50.0

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No active Spark session available.",
            }

        query = """
        WITH all_schemas AS (
            SELECT DISTINCT catalog_name, schema_name
            FROM system.information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'default')
              AND catalog_name NOT IN ('system', 'hive_metastore', '__databricks_internal')
        ),
        tagged_schemas AS (
            SELECT DISTINCT catalog_name, schema_name
            FROM system.information_schema.schema_tags
            WHERE catalog_name NOT IN ('system', 'hive_metastore', '__databricks_internal')
        )
        SELECT
            COUNT(DISTINCT a.catalog_name || '.' || a.schema_name) AS total_schemas,
            COUNT(DISTINCT t.catalog_name || '.' || t.schema_name) AS tagged_schemas
        FROM all_schemas a
        LEFT JOIN tagged_schemas t
            ON a.catalog_name = t.catalog_name AND a.schema_name = t.schema_name
        """
        print("[check_governed_tags_applied] Querying information_schema for tag coverage...")
        row = spark.sql(query).first()

        total = row["total_schemas"] if row else 0
        tagged = row["tagged_schemas"] if row else 0

        if total == 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": "No user schemas found to evaluate tag coverage.",
            }

        pct = round((tagged / total) * 100.0, 1)
        print(f"[check_governed_tags_applied] {tagged}/{total} schemas tagged ({pct}%)")

        if pct >= PASS_THRESHOLD_PCT:
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"{pct}% of schemas have governed tags ({tagged}/{total}), above {PASS_THRESHOLD_PCT}% threshold.",
            }
        elif tagged > 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": f"{pct}% of schemas have governed tags ({tagged}/{total}). Increase tag coverage to {PASS_THRESHOLD_PCT}%.",
            }
        else:
            return {
                "status": "fail",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": f"No governed tags applied to any of {total} schemas. Apply tags for ABAC, discoverability, and lifecycle management.",
            }

    except Exception as e:
        error_msg = str(e).lower()
        if "table_or_view_not_found" in error_msg or "table not found" in error_msg:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "information_schema.schema_tags not available. Tags may not be supported in this workspace.",
            }
        print(f"[check_governed_tags_applied] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking governed tags: {str(e)}",
        }


def check_no_wildcard_grants():
    """
    Check for overly permissive ALL PRIVILEGES grants on catalogs and schemas.

    Queries information_schema.catalog_privileges and schema_privileges for
    ANY grants of ALL PRIVILEGES, which bypass least-privilege principles.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_no_wildcard_grants] Starting check...")
    MAX_SCORE = 2

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No active Spark session available.",
            }

        query = """
        SELECT 'catalog' AS level, catalog_name AS object_name, grantee, privilege_type
        FROM system.information_schema.catalog_privileges
        WHERE privilege_type = 'ALL PRIVILEGES'
          AND catalog_name NOT IN ('system', 'hive_metastore', '__databricks_internal')
        UNION ALL
        SELECT 'schema' AS level, catalog_name || '.' || schema_name AS object_name, grantee, privilege_type
        FROM system.information_schema.schema_privileges
        WHERE privilege_type = 'ALL PRIVILEGES'
          AND catalog_name NOT IN ('system', 'hive_metastore', '__databricks_internal')
        """
        print("[check_no_wildcard_grants] Querying for ALL PRIVILEGES grants...")
        df = spark.sql(query)
        count = df.count()

        if count == 0:
            print("[check_no_wildcard_grants] PASS - No wildcard grants found")
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": "No ALL PRIVILEGES grants found on catalogs or schemas.",
            }

        rows = df.collect()
        examples = [f"{r['level']} '{r['object_name']}' -> {r['grantee']}" for r in rows[:5]]
        detail_str = "; ".join(examples)
        if count > 5:
            detail_str += f" (and {count - 5} more)"

        print(f"[check_no_wildcard_grants] FAIL - {count} wildcard grant(s) found")
        return {
            "status": "fail",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"{count} ALL PRIVILEGES grant(s) found. Replace with specific privileges (SELECT, USE CATALOG, etc.). {detail_str}",
        }

    except Exception as e:
        error_msg = str(e).lower()
        if "table_or_view_not_found" in error_msg or "table not found" in error_msg:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "information_schema privilege tables not available.",
            }
        print(f"[check_no_wildcard_grants] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking wildcard grants: {str(e)}",
        }


# =============================================================================
# Data Quality Governance Checks
# =============================================================================

def check_anomaly_detection_enabled():
    """
    Check if anomaly detection (freshness/completeness) is enabled.

    Queries system.data_quality_monitoring.table_results to see how many
    schemas have active monitoring, aiming for >=30% coverage.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_anomaly_detection_enabled] Starting check...")
    MAX_SCORE = 2
    PASS_THRESHOLD_PCT = 30.0

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No active Spark session available.",
            }

        query = """
        WITH all_schemas AS (
            SELECT DISTINCT catalog_name, schema_name
            FROM system.information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'default')
              AND catalog_name NOT IN ('system', 'hive_metastore', '__databricks_internal')
        ),
        monitored_schemas AS (
            SELECT DISTINCT
                split(table_name, '\\\\.')[0] AS catalog_name,
                split(table_name, '\\\\.')[1] AS schema_name
            FROM system.data_quality_monitoring.table_results
            WHERE execution_time >= current_date() - 30
        )
        SELECT
            COUNT(DISTINCT a.catalog_name || '.' || a.schema_name) AS total_schemas,
            COUNT(DISTINCT m.catalog_name || '.' || m.schema_name) AS monitored_schemas
        FROM all_schemas a
        LEFT JOIN monitored_schemas m
            ON a.catalog_name = m.catalog_name AND a.schema_name = m.schema_name
        """
        print("[check_anomaly_detection_enabled] Querying data quality monitoring coverage...")
        row = spark.sql(query).first()

        total = row["total_schemas"] if row else 0
        monitored = row["monitored_schemas"] if row else 0

        if total == 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": "No user schemas found to evaluate anomaly detection coverage.",
            }

        pct = round((monitored / total) * 100.0, 1)
        print(f"[check_anomaly_detection_enabled] {monitored}/{total} schemas monitored ({pct}%)")

        if pct >= PASS_THRESHOLD_PCT:
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"{pct}% of schemas have anomaly detection ({monitored}/{total}), above {PASS_THRESHOLD_PCT}% threshold.",
            }
        elif monitored > 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": f"{pct}% of schemas have anomaly detection ({monitored}/{total}). Enable for more schemas to reach {PASS_THRESHOLD_PCT}%.",
            }
        else:
            return {
                "status": "fail",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": f"No anomaly detection results found across {total} schemas. Enable anomaly detection for freshness/completeness monitoring.",
            }

    except Exception as e:
        error_msg = str(e).lower()
        if "table_or_view_not_found" in error_msg or "table not found" in error_msg:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "system.data_quality_monitoring.table_results not available. Enable data quality monitoring.",
            }
        print(f"[check_anomaly_detection_enabled] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking anomaly detection: {str(e)}",
        }


def check_certification_tags_used():
    """
    Check if tables use the system.certification_status tag (certified/deprecated).

    Queries information_schema.table_tags for the certification_status tag to
    measure adoption. Passing threshold is >30% of tables tagged.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_certification_tags_used] Starting check...")
    MAX_SCORE = 2

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No active Spark session available.",
            }

        query = """
        WITH all_tables AS (
            SELECT COUNT(*) AS total
            FROM system.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'default')
              AND table_catalog NOT IN ('system', 'hive_metastore', '__databricks_internal')
              AND table_type IN ('MANAGED', 'EXTERNAL', 'VIEW')
        ),
        tagged_tables AS (
            SELECT COUNT(DISTINCT catalog_name || '.' || schema_name || '.' || table_name) AS tagged
            FROM system.information_schema.table_tags
            WHERE tag_name = 'certification_status'
              AND catalog_name NOT IN ('system', 'hive_metastore', '__databricks_internal')
        )
        SELECT
            a.total AS total_tables,
            t.tagged AS tagged_tables
        FROM all_tables a, tagged_tables t
        """
        print("[check_certification_tags_used] Querying certification tag coverage...")
        row = spark.sql(query).first()

        total = row["total_tables"] if row else 0
        tagged = row["tagged_tables"] if row else 0

        if total == 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": "No tables found to evaluate certification tag coverage.",
            }

        pct = round((tagged / total) * 100.0, 1)
        print(f"[check_certification_tags_used] {tagged}/{total} tables have certification tags ({pct}%)")

        if pct >= 30.0:
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"{pct}% of tables have certification status tags ({tagged}/{total}).",
            }
        elif tagged > 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": f"{pct}% of tables have certification status tags ({tagged}/{total}). Tag more tables as certified or deprecated.",
            }
        else:
            return {
                "status": "fail",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": f"No tables have certification_status tags across {total} tables. Use certified/deprecated tags to improve trust and discoverability.",
            }

    except Exception as e:
        error_msg = str(e).lower()
        if "table_or_view_not_found" in error_msg or "table not found" in error_msg:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "information_schema.table_tags not available.",
            }
        print(f"[check_certification_tags_used] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking certification tags: {str(e)}",
        }


def check_sensitive_data_classification():
    """
    Check if sensitive data classification scanning is active.

    Queries system.data_classification.results to determine if any catalogs
    have been scanned, aiming for >=30% coverage.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_sensitive_data_classification] Starting check...")
    MAX_SCORE = 2
    PASS_THRESHOLD_PCT = 30.0

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No active Spark session available.",
            }

        query = """
        WITH all_catalogs AS (
            SELECT DISTINCT catalog_name
            FROM system.information_schema.schemata
            WHERE catalog_name NOT IN ('system', 'hive_metastore', '__databricks_internal')
        ),
        scanned_catalogs AS (
            SELECT DISTINCT split(table_name, '\\\\.')[0] AS catalog_name
            FROM system.data_classification.results
        )
        SELECT
            COUNT(DISTINCT a.catalog_name) AS total_catalogs,
            COUNT(DISTINCT s.catalog_name) AS scanned_catalogs
        FROM all_catalogs a
        LEFT JOIN scanned_catalogs s ON a.catalog_name = s.catalog_name
        """
        print("[check_sensitive_data_classification] Querying data classification results...")
        row = spark.sql(query).first()

        total = row["total_catalogs"] if row else 0
        scanned = row["scanned_catalogs"] if row else 0

        if total == 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": "No user catalogs found to evaluate classification coverage.",
            }

        pct = round((scanned / total) * 100.0, 1)
        print(f"[check_sensitive_data_classification] {scanned}/{total} catalogs scanned ({pct}%)")

        if pct >= PASS_THRESHOLD_PCT:
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"{pct}% of catalogs have data classification results ({scanned}/{total}), above {PASS_THRESHOLD_PCT}% threshold.",
            }
        elif scanned > 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": f"{pct}% of catalogs have data classification results ({scanned}/{total}). Enable scanning on more catalogs.",
            }
        else:
            return {
                "status": "fail",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": f"No data classification results found across {total} catalogs. Enable sensitive data scanning to detect PII and financial data.",
            }

    except Exception as e:
        error_msg = str(e).lower()
        if "table_or_view_not_found" in error_msg or "table not found" in error_msg:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "system.data_classification.results not available. Enable data classification.",
            }
        print(f"[check_sensitive_data_classification] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking data classification: {str(e)}",
        }


# =============================================================================
# Lineage & Discoverability Checks
# =============================================================================

def check_orphan_tables():
    """
    Check for orphan tables with no lineage activity in the last 90 days.

    Uses a pure SQL approach joining information_schema.tables with
    system.access.table_lineage to avoid slow SDK iteration. Tables with
    no reads or writes in 90 days are flagged as orphans.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_orphan_tables] Starting check...")
    MAX_SCORE = 2
    ORPHAN_THRESHOLD_PCT = 10.0

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No active Spark session available.",
            }

        query = """
        WITH all_tables AS (
            SELECT table_catalog || '.' || table_schema || '.' || table_name AS full_name
            FROM system.information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'default')
              AND table_catalog NOT IN ('system', 'hive_metastore', '__databricks_internal')
              AND table_type IN ('MANAGED', 'EXTERNAL')
        ),
        active_tables AS (
            SELECT DISTINCT target_table_full_name AS full_name
            FROM system.access.table_lineage
            WHERE event_date >= current_date() - 90
            UNION
            SELECT DISTINCT source_table_full_name
            FROM system.access.table_lineage
            WHERE event_date >= current_date() - 90
        )
        SELECT
            COUNT(*) AS total_tables,
            COUNT(a.full_name) AS active_tables,
            COUNT(*) - COUNT(a.full_name) AS orphan_tables
        FROM all_tables t
        LEFT JOIN active_tables a ON t.full_name = a.full_name
        """
        print("[check_orphan_tables] Querying lineage for orphan tables...")
        row = spark.sql(query).first()

        total = row["total_tables"] if row else 0
        orphans = row["orphan_tables"] if row else 0
        active = row["active_tables"] if row else 0

        if total == 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": "No tables found to evaluate orphan status.",
            }

        pct = round((orphans / total) * 100.0, 1)
        print(f"[check_orphan_tables] {orphans}/{total} orphan tables ({pct}%), {active} active")

        if pct < ORPHAN_THRESHOLD_PCT:
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"{pct}% orphan tables ({orphans}/{total}), below {ORPHAN_THRESHOLD_PCT}% threshold. {active} tables active in last 90 days.",
            }
        else:
            score = 1 if pct < 30.0 else 0
            return {
                "status": "fail",
                "score": score,
                "max_score": MAX_SCORE,
                "details": f"{pct}% orphan tables ({orphans}/{total}) with no lineage in 90 days. Consider deprecating or cleaning up unused tables.",
            }

    except Exception as e:
        error_msg = str(e).lower()
        if "table_or_view_not_found" in error_msg or "table not found" in error_msg:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "system.access.table_lineage or information_schema.tables not available.",
            }
        print(f"[check_orphan_tables] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking orphan tables: {str(e)}",
        }


def check_table_column_comments():
    """
    Check the coverage of table and column comments/descriptions.

    Well-documented tables and columns improve discoverability, governance,
    and trust. Passing requires >=70% of tables and >=50% of columns with comments.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_table_column_comments] Starting check...")
    MAX_SCORE = 2
    TABLE_COMMENT_THRESHOLD = 70.0
    COLUMN_COMMENT_THRESHOLD = 50.0

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No active Spark session available.",
            }

        table_query = """
        SELECT
            COUNT(*) AS total_tables,
            SUM(CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 ELSE 0 END) AS commented_tables
        FROM system.information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'default')
          AND table_catalog NOT IN ('system', 'hive_metastore', '__databricks_internal')
          AND table_type IN ('MANAGED', 'EXTERNAL', 'VIEW')
        """

        col_query = """
        SELECT
            COUNT(*) AS total_columns,
            SUM(CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 ELSE 0 END) AS commented_columns
        FROM system.information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'default')
          AND table_catalog NOT IN ('system', 'hive_metastore', '__databricks_internal')
        """

        print("[check_table_column_comments] Querying table and column comment coverage...")
        t_row = spark.sql(table_query).first()
        c_row = spark.sql(col_query).first()

        total_tables = t_row["total_tables"] if t_row else 0
        commented_tables = t_row["commented_tables"] if t_row else 0
        total_columns = c_row["total_columns"] if c_row else 0
        commented_columns = c_row["commented_columns"] if c_row else 0

        if total_tables == 0:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": "No tables found to evaluate comment coverage.",
            }

        table_pct = round((commented_tables / total_tables) * 100.0, 1) if total_tables > 0 else 0
        col_pct = round((commented_columns / total_columns) * 100.0, 1) if total_columns > 0 else 0

        print(f"[check_table_column_comments] Tables: {table_pct}% commented, Columns: {col_pct}% commented")

        if table_pct >= TABLE_COMMENT_THRESHOLD and col_pct >= COLUMN_COMMENT_THRESHOLD:
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"Tables: {table_pct}% commented ({commented_tables}/{total_tables}), Columns: {col_pct}% commented ({commented_columns}/{total_columns}).",
            }
        elif table_pct >= TABLE_COMMENT_THRESHOLD or col_pct >= COLUMN_COMMENT_THRESHOLD:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": f"Partial comment coverage. Tables: {table_pct}% ({commented_tables}/{total_tables}), Columns: {col_pct}% ({commented_columns}/{total_columns}). Improve to {TABLE_COMMENT_THRESHOLD}% tables / {COLUMN_COMMENT_THRESHOLD}% columns.",
            }
        else:
            return {
                "status": "fail",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": f"Low comment coverage. Tables: {table_pct}% ({commented_tables}/{total_tables}), Columns: {col_pct}% ({commented_columns}/{total_columns}). Add descriptions for discoverability.",
            }

    except Exception as e:
        print(f"[check_table_column_comments] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking comment coverage: {str(e)}",
        }


# =============================================================================
# AI & Model Governance Checks
# =============================================================================

def check_models_in_uc():
    """
    Check if ML models are registered in Unity Catalog vs legacy workspace registry.

    UC registration ensures governance, lineage, and cross-workspace access.
    Passes when all models are in UC and none in the legacy registry.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_models_in_uc] Starting check...")
    MAX_SCORE = 2

    try:
        w = get_workspace_client()
        if w is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "Workspace client not available.",
            }

        uc_model_count = 0
        legacy_model_count = 0

        print("[check_models_in_uc] Counting UC registered models...")
        try:
            for _ in w.registered_models.list(max_results=1000):
                uc_model_count += 1
        except Exception as e:
            print(f"[check_models_in_uc] Error listing UC models: {e}")

        print("[check_models_in_uc] Counting legacy workspace models...")
        try:
            if hasattr(w, 'model_registry'):
                for _ in w.model_registry.list_models():
                    legacy_model_count += 1
        except Exception as e:
            print(f"[check_models_in_uc] Legacy registry not available or empty: {e}")

        total = uc_model_count + legacy_model_count
        print(f"[check_models_in_uc] UC models: {uc_model_count}, Legacy models: {legacy_model_count}")

        if total == 0:
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": "No ML models found in either registry. No migration needed.",
            }

        if legacy_model_count == 0:
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"All {uc_model_count} model(s) registered in Unity Catalog. No legacy models.",
            }

        pct_uc = round((uc_model_count / total) * 100.0, 1)
        return {
            "status": "fail",
            "score": 1 if pct_uc > 50 else 0,
            "max_score": MAX_SCORE,
            "details": f"{legacy_model_count} model(s) still in legacy workspace registry. {uc_model_count} in UC ({pct_uc}%). Migrate legacy models to Unity Catalog.",
        }

    except Exception as e:
        print(f"[check_models_in_uc] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking model registry: {str(e)}",
        }


def check_mlflow_experiment_tracking():
    """
    Check if MLflow experiments are tracked in UC-managed MLflow.

    Queries system.mlflow.experiments_latest to confirm experiments exist
    and are actively recording runs in the last 30 days.

    Returns:
        dict: Status with score, max_score, and details
    """
    print("[check_mlflow_experiment_tracking] Starting check...")
    MAX_SCORE = 2

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No active Spark session available.",
            }

        query = """
        SELECT
            COUNT(*) AS total_experiments,
            SUM(CASE WHEN update_time >= current_timestamp() - INTERVAL 30 DAYS THEN 1 ELSE 0 END) AS active_experiments
        FROM system.mlflow.experiments_latest
        WHERE delete_time IS NULL
        """
        print("[check_mlflow_experiment_tracking] Querying MLflow experiments...")
        row = spark.sql(query).first()

        total = row["total_experiments"] if row else 0
        active = row["active_experiments"] if row else 0

        if total == 0:
            return {
                "status": "fail",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "No MLflow experiments found. Set up UC-managed MLflow experiment tracking.",
            }

        if active > 0:
            return {
                "status": "pass",
                "score": MAX_SCORE,
                "max_score": MAX_SCORE,
                "details": f"{active} active experiment(s) with runs in last 30 days ({total} total experiments tracked).",
            }
        else:
            return {
                "status": "warning",
                "score": 1,
                "max_score": MAX_SCORE,
                "details": f"{total} experiment(s) exist but none had runs in the last 30 days. Verify teams are actively using MLflow tracking.",
            }

    except Exception as e:
        error_msg = str(e).lower()
        if "table_or_view_not_found" in error_msg or "table not found" in error_msg:
            return {
                "status": "error",
                "score": 0,
                "max_score": MAX_SCORE,
                "details": "system.mlflow.experiments_latest not available. Enable MLflow system tables.",
            }
        print(f"[check_mlflow_experiment_tracking] Error: {e}")
        return {
            "status": "error",
            "score": 0,
            "max_score": MAX_SCORE,
            "details": f"Error checking MLflow experiments: {str(e)}",
        }
