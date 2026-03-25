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

checks = ga.GOVERNANCE_CHECKS

# COMMAND ----------

# Run all checks
results = []
timestamp = datetime.now()

for category, task_name, check_func, remediation_text in checks:
    result = check_func()
    max_score = result["max_score"]
    score = result["score"]
    score_percentage = round((score / max_score * 100.0) if max_score > 0 else 0.0, 2)
    
    remediation = remediation_text if result["status"] != "pass" else ""

    results.append({
        "timestamp": timestamp,
        "category": category,
        "task_name": task_name,
        "status": result["status"],
        "score": score,
        "max_score": max_score,
        "score_percentage": float(score_percentage),
        "details": result["details"],
        "remediation": remediation,
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
