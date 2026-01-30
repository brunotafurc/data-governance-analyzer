"""Dashboard creation and deployment for governance results."""

import json
import os


def create_dashboard(catalog_name, schema_name, folder_path="/Shared/Governance", dashboard_name="Governance Results Dashboard"):
    """
    Create and deploy a Lakeview dashboard for governance results using Databricks SDK.
    
    Args:
        catalog_name: The catalog containing the governance_results table
        schema_name: The schema containing the governance_results table
        folder_path: The workspace folder path where the dashboard will be created
        dashboard_name: The name for the dashboard
    
    Returns:
        dict: Dashboard creation response with dashboard_id and path
    """
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.dashboards import Dashboard
    except ImportError:
        return {
            "status": "error",
            "message": "Databricks SDK not available. Please install: pip install databricks-sdk"
        }
    
    # Get workspace context
    try:
        from databricks.sdk.runtime import dbutils
        workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    except:
        workspace_url = None
    
    print("Loading dashboard template...")
    # Load dashboard template
    template_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard_template.lvdash.json")
    with open(template_path, 'r') as f:
        dashboard_spec = json.load(f)
    
    # Update the query to use the correct catalog and schema
    full_table_name = f"{catalog_name}.{schema_name}.governance_results"
    dashboard_spec["datasets"][0]["queryLines"] = [
        f"SELECT category, details, score, status, task_name, timestamp, max_score, score_percentage, concat(score,'/',max_score) as fraction_score  FROM {full_table_name}"
    ]
    
    try:
        print("Initializing Databricks SDK...")
        # Initialize Databricks SDK WorkspaceClient
        w = WorkspaceClient()
        
        # Ensure folder exists
        print(f"Ensuring folder exists: {folder_path}")
        if folder_path:
            try:
                w.workspace.mkdirs(path=folder_path)
            except Exception as e:
                print(f"Note: Folder may already exist: {str(e)}")
        
        # Try to create the dashboard directly - if it fails due to existing, we'll handle it
        print("Preparing dashboard object...")
        dashboard_obj = Dashboard(
            display_name=dashboard_name,
            parent_path=folder_path,
            serialized_dashboard=json.dumps(dashboard_spec)
        )
        
        action = "created"
        dashboard_id = None
        dashboard_path = None
        
        try:
            # Try to create new dashboard
            print("Calling lakeview.create()...")
            created_dashboard = w.lakeview.create(
                dashboard=dashboard_obj
            )
            print("Dashboard created successfully")
            
            dashboard_id = created_dashboard.dashboard_id
            dashboard_path = created_dashboard.path
            
        except Exception as create_error:
            # If creation fails due to existing dashboard, try to delete the workspace node
            error_msg = str(create_error)
            print(f"Create failed: {error_msg}")
            if "already exists" in error_msg.lower():
                print(f"Dashboard file exists in workspace, attempting to delete it...")
                
                # Try to delete the workspace file directly
                dashboard_file_path = f"{folder_path}/{dashboard_name}.lvdash.json"
                try:
                    print(f"Deleting workspace file: {dashboard_file_path}")
                    w.workspace.delete(path=dashboard_file_path)
                    print("Workspace file deleted successfully")
                    
                    # Now try to create the dashboard again
                    print("Retrying dashboard creation...")
                    created_dashboard = w.lakeview.create(
                        dashboard=dashboard_obj
                    )
                    dashboard_id = created_dashboard.dashboard_id
                    dashboard_path = created_dashboard.path
                    action = "replaced"
                    print("Dashboard created successfully after removing old file")
                    
                except Exception as delete_error:
                    print(f"Could not delete workspace file: {str(delete_error)}")
                    
                    # Fallback: try to find dashboard by listing
                    print("Listing dashboards to find existing one...")
                    try:
                        count = 0
                        found_dashboard = None
                        for db in w.lakeview.list():
                            count += 1
                            if db.display_name == dashboard_name:
                                found_dashboard = db
                                print(f"Found existing dashboard: {db.display_name} (ID: {db.dashboard_id})")
                                break
                            if count >= 50:  # Limit search to avoid timeout
                                print(f"Searched {count} dashboards, stopping...")
                                break
                        
                        if found_dashboard:
                            # Just return the existing dashboard info
                            dashboard_id = found_dashboard.dashboard_id
                            dashboard_path = found_dashboard.path
                            action = "found existing"
                            print(f"Using existing dashboard: {dashboard_id}")
                        else:
                            raise Exception(f"Dashboard exists but could not be found or deleted: {error_msg}")
                            
                    except Exception as list_error:
                        raise Exception(f"Could not resolve dashboard conflict: {str(list_error)}")
            else:
                # Re-raise if not an "already exists" error
                raise
        
        # Publish the dashboard
        if dashboard_id:
            print(f"Publishing dashboard {dashboard_id}...")
            try:
                w.lakeview.publish(dashboard_id=dashboard_id)
                print("Dashboard published successfully")
            except Exception as e:
                print(f"Note: Could not publish dashboard: {str(e)}")
        
        # Build dashboard URL
        if workspace_url:
            dashboard_url = f"{workspace_url}/sql/dashboardsv3/{dashboard_id}"
        else:
            dashboard_url = f"<workspace_url>/sql/dashboardsv3/{dashboard_id}"
        
        return {
            "status": "success",
            "dashboard_id": dashboard_id,
            "dashboard_path": dashboard_path or f"{folder_path}/{dashboard_name}",
            "workspace_url": dashboard_url,
            "message": f"Dashboard {action} successfully at {dashboard_path or folder_path}"
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "message": f"Failed to create dashboard: {str(e)}"
        }
