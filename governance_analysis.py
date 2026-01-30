# Databricks notebook source
# MAGIC %md
# MAGIC # Data Governance Analysis
# MAGIC This notebook runs all governance checks and saves results to a Delta table

# COMMAND ----------

# Install required dependencies
%pip install databricks-sdk --upgrade --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "default", "Schema Name")
dbutils.widgets.text("account_id", "", "Account ID (optional)")
dbutils.widgets.text("client_id", "", "Client ID (optional)")
dbutils.widgets.text("client_secret", "", "Client Secret (optional)")

# Get parameter values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
account_id = dbutils.widgets.get("account_id") or None
client_id = dbutils.widgets.get("client_id") or None
client_secret = dbutils.widgets.get("client_secret") or None

print(f"Using catalog: {catalog_name}")
print(f"Using schema: {schema_name}")
if account_id:
    print(f"Account-level authentication: enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Save scores to delta table

# COMMAND ----------

import governance_analyzer as ga
from datetime import datetime

# Configure account-level authentication if credentials provided
if account_id:
    ga.configure_account_auth(
        account_id=account_id,
        client_id=client_id,
        client_secret=client_secret
    )

# COMMAND ----------

# Define governance checks
checks = [
    ("Metastore setup", "Connect a Metastore to your Workspace", ga.check_metastore_connected),
    ("Metastore setup", "The workspace is in the same region as the metastore", ga.check_metastore_region),
    ("Identity", "Use SCIM or AIM from an Identity Provider", ga.check_scim_aim_provisioning),
    ("Identity", "Account Admin role is assigned to a group", ga.check_account_admin_group),
    ("Identity", "Metastore Admin role is assigned to a group", ga.check_metastore_admin_group),
    ("Identity", "Workspace Admin role is assigned to a group", ga.check_workspace_admin_group),
    ("Identity", "Catalog Admin role is assigned to a group", ga.check_catalog_admin_group),
    ("Identity", "At least 1 user is an account admin", ga.check_at_least_one_account_admin),
    ("Identity", "Less than 5% of users are Account Admin", ga.check_account_admin_percentage),
    ("Managed Storage", "Create multiple Catalogs based on environment/BU/team", ga.check_multiple_catalogs),
    ("Managed Storage", "No Catalog is bound to all workspaces", ga.check_catalog_binding),
    ("Managed Storage", "Use Managed tables and volumes > 70%", ga.check_managed_tables_percentage),
    ("Managed Storage", "No ADLS or S3 buckets outside UC", ga.check_no_external_storage),
    ("Managed Storage", "No external volumes/tables at external location root", ga.check_external_location_root),
    ("Managed Storage", "Independent storage credentials per external location", ga.check_storage_credentials),
    ("Compute/Cluster Policy", "Compute is UC activated with right access mode", ga.check_uc_compute),
    ("Migration Completeness", "No data in hive metastore", ga.check_no_hive_data),
    ("Migration Completeness", "Hive metastore is disabled", ga.check_hive_disabled),
    ("Migration Completeness", "0 mount storage accounts to DBFS", ga.check_no_dbfs_mounts),
    ("Audit & Lineage Coverage", "All system tables activated (70%)", ga.check_system_tables),
    ("Audit & Lineage Coverage", "70% of managed tables have predictive optimization", ga.check_predictive_optimization),
    ("Audit & Lineage Coverage", "Data quality activated on 50% of tables", ga.check_data_quality),
    ("Privileges", "Production jobs use service principals", ga.check_service_principals),
    ("Privileges", "Modify access to production is limited", ga.check_production_access),
    ("Privileges", "70% of assets have groups as owners", ga.check_group_ownership),
]

# COMMAND ----------

# Run all checks
results = []
timestamp = datetime.now()

for category, task_name, check_func in checks:
    result = check_func()
    max_score = result["max_score"]
    score = result["score"]
    score_percentage = round((score / max_score * 100.0) if max_score > 0 else 0.0, 2)
    
    results.append({
        "timestamp": timestamp,
        "category": category,
        "task_name": task_name,
        "status": result["status"],
        "score": score,
        "max_score": max_score,
        "score_percentage": float(score_percentage),
        "details": result["details"]
    })

print(f"Completed {len(results)} governance checks")

# COMMAND ----------

# Convert to DataFrame
df = spark.createDataFrame(results)
display(df)

# COMMAND ----------

# Save to Delta table
table_name = "governance_results"
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(full_table_name)

print(f"✓ Results saved to {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deploy Dashboard

# COMMAND ----------

# Deploy Lakeview Dashboard
print("Creating Lakeview Dashboard...")
print("Note: This may take up to 30 seconds...")

import threading

def create_dashboard_with_timeout():
    result = {"status": "timeout", "message": "Dashboard creation timed out after 30 seconds"}
    
    def run_creation():
        nonlocal result
        try:
            result = ga.create_dashboard(
                catalog_name=catalog_name,
                schema_name=schema_name,
                folder_path="/Shared/Governance",
                dashboard_name=f"Governance Results Dashboard - {catalog_name}.{schema_name}"
            )
        except Exception as e:
            result = {
                "status": "error",
                "message": str(e),
                "error": str(e)
            }
    
    thread = threading.Thread(target=run_creation)
    thread.daemon = True
    thread.start()
    thread.join(timeout=30)  # 30 second timeout
    
    if thread.is_alive():
        print("⚠ Dashboard creation is taking too long and was stopped")
        return {"status": "timeout", "message": "Operation timed out"}
    
    return result

try:
    dashboard_result = create_dashboard_with_timeout()
    
    if dashboard_result["status"] == "success":
        print(f"✓ {dashboard_result['message']}")
        print(f"  Dashboard URL: {dashboard_result['workspace_url']}")
        print(f"  Dashboard Path: {dashboard_result['dashboard_path']}")
        displayHTML(f'<a href="{dashboard_result["workspace_url"]}" target="_blank">Open Dashboard</a>')
    else:
        print(f"⚠ Dashboard creation failed: {dashboard_result['message']}")
        if "error" in dashboard_result:
            print(f"  Error details: {dashboard_result['error']}")
        print(f"\n  Manual import instructions:")
        print(f"  1. Download dashboard_template.lvdash.json from this workspace")
        print(f"  2. Go to SQL Workspace → Dashboards → Import")
        print(f"  3. Upload the file and update the dataset query to: {catalog_name}.{schema_name}.governance_results")
except Exception as e:
    print(f"⚠ Could not create dashboard: {str(e)}")
    import traceback
    print(f"\n  Error traceback:")
    traceback.print_exc()
    print(f"\n  Manual import instructions:")
    print(f"  1. Download dashboard_template.lvdash.json from this workspace")
    print(f"  2. Go to SQL Workspace → Dashboards → Import")
    print(f"  3. Upload the file and update the dataset query to: {catalog_name}.{schema_name}.governance_results")
