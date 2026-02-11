# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Checks — Test Asset Setup
# MAGIC
# MAGIC This notebook creates **all** the infrastructure needed to test the 5 governance checks.
# MAGIC
# MAGIC ### What YOU need before running:
# MAGIC 1. An **ADLS Gen 2** storage account + a **test container** (e.g., `governance-test`)
# MAGIC 2. **One of these** for authentication (pick the one that fits your org):
# MAGIC
# MAGIC | Option | What you need | When to use |
# MAGIC |--------|---------------|-------------|
# MAGIC | **A) Access Connector** (recommended) | Resource ID of your Access Connector + `Storage Blob Data Contributor` on the storage account | You already have an Access Connector (most UC workspaces do) |
# MAGIC | **B) Service Principal** | Client ID + Secret + Tenant ID + `Storage Blob Data Contributor` on the storage account | You prefer explicit credentials |
# MAGIC | **C) Existing credential** | Name of a Storage Credential already in Unity Catalog | You already set one up |
# MAGIC
# MAGIC ### What this notebook creates:
# MAGIC | Asset | Purpose | Check it tests |
# MAGIC |-------|---------|----------------|
# MAGIC | 2 catalogs (PO on/off) + schemas + 10 managed tables | Predictive Optimization | `check_predictive_optimization` |
# MAGIC | 1 Storage Credential | Access ADLS from UC | `check_storage_credentials` |
# MAGIC | 2 External Locations (same credential) | Shared credential = FAIL | `check_storage_credentials` |
# MAGIC | 1 External table at location root | Object at root = FAIL | `check_external_location_root` |
# MAGIC | 1 DBFS mount | Legacy mount = FAIL | `check_no_dbfs_mounts` |
# MAGIC | Lakehouse Monitors on some tables | < 50% monitored = FAIL | `check_data_quality` |
# MAGIC
# MAGIC ### Expected results:
# MAGIC | Check | Expected | Why |
# MAGIC |-------|----------|-----|
# MAGIC | Predictive Optimization | ✅ PASS (70%) | 7/10 tables in PO-enabled catalog |
# MAGIC | DBFS Mounts | ❌ FAIL | We create 1 mount |
# MAGIC | External Location Root | ❌ FAIL | We place a table at the root |
# MAGIC | Storage Credentials | ❌ FAIL | 2 locations share 1 credential |
# MAGIC | Data Quality | ❌ FAIL (40%) | Only 4/10 tables monitored |

# COMMAND ----------

%pip install databricks-sdk --upgrade --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Fill in the widgets. The **prefix** is always required.
# MAGIC
# MAGIC - **Core tests only** (Predictive Optimization): just the prefix.
# MAGIC - **All tests**: prefix + ADLS URL + one auth method (Access Connector OR Service Principal OR existing credential).
# MAGIC
# MAGIC ### How to find your Access Connector Resource ID:
# MAGIC Azure Portal → your Resource Group → look for `Access Connector for Azure Databricks` →
# MAGIC Properties → **Resource ID** (looks like `/subscriptions/.../resourceGroups/.../providers/Microsoft.Databricks/accessConnectors/...`)

# COMMAND ----------

# -- Widgets ----------------------------------------------------------------
dbutils.widgets.text("prefix", "gov_test", "1. Resource prefix")
dbutils.widgets.text("adls_container_url", "", "2. ADLS URL: abfss://container@account.dfs.core.windows.net")

# Auth option A: Access Connector (recommended for Azure)
dbutils.widgets.text("access_connector_id", "", "3a. Access Connector Resource ID (recommended)")

# Auth option B: Service Principal
dbutils.widgets.text("sp_client_id", "", "3b. Service Principal Client ID")
dbutils.widgets.text("sp_client_secret", "", "3b. Service Principal Client Secret")
dbutils.widgets.text("sp_tenant_id", "", "3b. Service Principal Tenant ID")

# Auth option C: Existing credential
dbutils.widgets.text("existing_credential_name", "", "3c. Existing Storage Credential name")

# Optional
dbutils.widgets.text("warehouse_id", "", "4. SQL Warehouse ID (for monitors, optional)")

# -- Read parameters --------------------------------------------------------
PREFIX = dbutils.widgets.get("prefix").strip()
ADLS_URL = dbutils.widgets.get("adls_container_url").strip().rstrip("/") or None
ACCESS_CONNECTOR_ID = dbutils.widgets.get("access_connector_id").strip() or None
SP_CLIENT_ID = dbutils.widgets.get("sp_client_id").strip() or None
SP_CLIENT_SECRET = dbutils.widgets.get("sp_client_secret").strip() or None
SP_TENANT_ID = dbutils.widgets.get("sp_tenant_id").strip() or None
EXISTING_CREDENTIAL = dbutils.widgets.get("existing_credential_name").strip() or None
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip() or None

# Determine which auth method is configured
SP_CONFIGURED = all([SP_CLIENT_ID, SP_CLIENT_SECRET, SP_TENANT_ID])
AUTH_METHOD = None
if EXISTING_CREDENTIAL:
    AUTH_METHOD = "existing"
elif ACCESS_CONNECTOR_ID:
    AUTH_METHOD = "access_connector"
elif SP_CONFIGURED:
    AUTH_METHOD = "service_principal"

STORAGE_CONFIGURED = ADLS_URL and AUTH_METHOD is not None

print(f"Prefix:             {PREFIX}")
print(f"ADLS URL:           {ADLS_URL or '(not set)'}")
print(f"Auth method:        ", end="")
if AUTH_METHOD == "existing":
    print(f"Existing credential '{EXISTING_CREDENTIAL}' ✓")
elif AUTH_METHOD == "access_connector":
    print(f"Access Connector ✓")
elif AUTH_METHOD == "service_principal":
    print(f"Service Principal ✓")
else:
    print("(not configured — external location + mount tests will be skipped)")
print(f"Warehouse ID:       {WAREHOUSE_ID or '(not set — data quality monitor tests will be skipped)'}")
print(f"Storage tests:      {'ENABLED ✓' if STORAGE_CONFIGURED else 'SKIPPED (need ADLS URL + auth method)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Core Setup — Catalogs, Schemas, Managed Tables, Predictive Optimization
# MAGIC
# MAGIC **No cloud infrastructure needed.** This section creates Unity Catalog objects
# MAGIC to test `check_predictive_optimization`.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import EnablePredictiveOptimization

w = WorkspaceClient()

# -- Naming conventions -----------------------------------------------------
CAT_PO_ON = f"{PREFIX}_po_on"
CAT_PO_OFF = f"{PREFIX}_po_off"
SCHEMA_NAME = "test_data"

# -- Create catalogs --------------------------------------------------------
# Enterprise workspaces with Default Storage enabled require an explicit
# MANAGED LOCATION for each catalog (no metastore root storage).
# We use the ADLS container URL if provided, with a subfolder per catalog.
for cat_name in [CAT_PO_ON, CAT_PO_OFF]:
    try:
        if ADLS_URL:
            managed_location = f"{ADLS_URL}/{cat_name}"
            w.catalogs.create(name=cat_name, storage_root=managed_location)
            print(f"✓ Created catalog: {cat_name}")
            print(f"  Managed location: {managed_location}")
        else:
            # Try without location (works if metastore has a default root)
            w.catalogs.create(name=cat_name)
            print(f"✓ Created catalog: {cat_name} (using metastore default storage)")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"• Catalog already exists: {cat_name}")
        elif "storage root url" in str(e).lower() or "managed location" in str(e).lower():
            print(f"✗ Error: Metastore has no default storage root.")
            print(f"  Please provide the 'adls_container_url' widget so catalogs can be created")
            print(f"  with an explicit managed location.")
            raise
        else:
            raise

# -- Configure Predictive Optimization -------------------------------------
w.catalogs.update(
    name=CAT_PO_ON,
    enable_predictive_optimization=EnablePredictiveOptimization.ENABLE,
)
print(f"✓ Predictive Optimization ENABLED on {CAT_PO_ON}")

w.catalogs.update(
    name=CAT_PO_OFF,
    enable_predictive_optimization=EnablePredictiveOptimization.DISABLE,
)
print(f"✓ Predictive Optimization DISABLED on {CAT_PO_OFF}")

# -- Create schemas ---------------------------------------------------------
for cat_name in [CAT_PO_ON, CAT_PO_OFF]:
    try:
        w.schemas.create(name=SCHEMA_NAME, catalog_name=cat_name)
        print(f"✓ Created schema: {cat_name}.{SCHEMA_NAME}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"• Schema already exists: {cat_name}.{SCHEMA_NAME}")
        else:
            raise

# COMMAND ----------

# -- Create managed tables ---------------------------------------------------
# 7 tables in PO-ON catalog + 3 in PO-OFF catalog = 70% with PO (threshold)

PO_ON_TABLES = [f"managed_tbl_{i}" for i in range(1, 8)]   # 7 tables
PO_OFF_TABLES = [f"managed_tbl_{i}" for i in range(1, 4)]  # 3 tables

for cat_name, table_list in [(CAT_PO_ON, PO_ON_TABLES), (CAT_PO_OFF, PO_OFF_TABLES)]:
    for tbl in table_list:
        full_name = f"`{cat_name}`.`{SCHEMA_NAME}`.`{tbl}`"
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_name} (
                    id BIGINT,
                    name STRING,
                    value DOUBLE,
                    created_at TIMESTAMP
                )
            """)
            spark.sql(f"""
                INSERT INTO {full_name} VALUES
                (1, 'alpha', 10.5, current_timestamp()),
                (2, 'beta',  20.3, current_timestamp()),
                (3, 'gamma', 30.1, current_timestamp())
            """)
            print(f"✓ Created + populated: {cat_name}.{SCHEMA_NAME}.{tbl}")
        except Exception as e:
            print(f"✗ Error creating {full_name}: {e}")

# COMMAND ----------

total = len(PO_ON_TABLES) + len(PO_OFF_TABLES)
pct = round(len(PO_ON_TABLES) / total * 100, 1)

print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  ✓ CORE SETUP COMPLETE                                         ║
╠══════════════════════════════════════════════════════════════════╣
║  Catalogs: {CAT_PO_ON} (PO=ON), {CAT_PO_OFF} (PO=OFF)
║  Schema:   {SCHEMA_NAME}
║  Tables:   {len(PO_ON_TABLES)} in PO-ON + {len(PO_OFF_TABLES)} in PO-OFF = {total}
║  Expected: check_predictive_optimization → PASS ({pct}% ≥ 70%)
╚══════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Storage Credential + External Locations
# MAGIC
# MAGIC **Requires:** ADLS URL + one of: Access Connector / Service Principal / existing credential name.
# MAGIC
# MAGIC This section creates:
# MAGIC 1. A **Storage Credential** in Unity Catalog (skipped if using an existing one)
# MAGIC 2. Two **External Locations** pointing to different paths but using the **same** credential
# MAGIC 3. An **External Table at the root** of one external location
# MAGIC
# MAGIC This makes `check_storage_credentials` **FAIL** (shared credential)
# MAGIC and `check_external_location_root` **FAIL** (table at root).

# COMMAND ----------

if STORAGE_CONFIGURED:
    EXT_LOC_1 = f"{PREFIX}_ext_loc_1"
    EXT_LOC_2 = f"{PREFIX}_ext_loc_2"
    EXT_URL_1 = f"{ADLS_URL}/{PREFIX}_loc1"
    EXT_URL_2 = f"{ADLS_URL}/{PREFIX}_loc2"

    # -- Step 1: Resolve or create the Storage Credential --------------------
    if AUTH_METHOD == "existing":
        # Option C: Use a credential that already exists in Unity Catalog
        CRED_NAME = EXISTING_CREDENTIAL
        print(f"── Using existing Storage Credential: {CRED_NAME} ──")

    elif AUTH_METHOD == "access_connector":
        # Option A: Create credential using Azure Access Connector (Managed Identity)
        from databricks.sdk.service.catalog import AzureManagedIdentity

        CRED_NAME = f"{PREFIX}_credential"
        print(f"── Creating Storage Credential via Access Connector ──")
        try:
            w.storage_credentials.create(
                name=CRED_NAME,
                azure_managed_identity=AzureManagedIdentity(
                    access_connector_id=ACCESS_CONNECTOR_ID,
                ),
                comment=f"Test credential for governance checks (prefix: {PREFIX})",
            )
            print(f"✓ Created storage credential: {CRED_NAME}")
            print(f"  Access Connector: {ACCESS_CONNECTOR_ID}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"• Storage credential already exists: {CRED_NAME}")
            else:
                print(f"✗ Error creating storage credential: {e}")
                print("  Make sure:")
                print("  1. The Access Connector resource ID is correct")
                print("  2. The Access Connector's Managed Identity has 'Storage Blob Data Contributor'")
                print("     role on your ADLS Gen 2 storage account")
                raise

    elif AUTH_METHOD == "service_principal":
        # Option B: Create credential using Service Principal
        from databricks.sdk.service.catalog import AzureServicePrincipal

        CRED_NAME = f"{PREFIX}_credential"
        print(f"── Creating Storage Credential via Service Principal ──")
        try:
            w.storage_credentials.create(
                name=CRED_NAME,
                azure_service_principal=AzureServicePrincipal(
                    directory_id=SP_TENANT_ID,
                    application_id=SP_CLIENT_ID,
                    client_secret=SP_CLIENT_SECRET,
                ),
                comment=f"Test credential for governance checks (prefix: {PREFIX})",
            )
            print(f"✓ Created storage credential: {CRED_NAME}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"• Storage credential already exists: {CRED_NAME}")
            else:
                print(f"✗ Error creating storage credential: {e}")
                print("  Make sure your Service Principal has 'Storage Blob Data Contributor' role")
                print("  on the ADLS Gen 2 storage account.")
                raise

    # -- Step 2: Create 2 External Locations with SAME credential (fail case) -
    print("\n── Creating External Locations (shared credential → FAIL case) ──")
    for loc_name, loc_url in [(EXT_LOC_1, EXT_URL_1), (EXT_LOC_2, EXT_URL_2)]:
        try:
            w.external_locations.create(
                name=loc_name,
                url=loc_url,
                credential_name=CRED_NAME,
                skip_validation=True,
                comment=f"Test location for governance checks",
            )
            print(f"✓ Created external location: {loc_name}")
            print(f"  URL: {loc_url}")
            print(f"  Credential: {CRED_NAME}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"• External location already exists: {loc_name}")
            else:
                print(f"✗ Error creating {loc_name}: {e}")

    # -- Step 3: Create external table AT root of ext location 1 (fail case) -
    print("\n── Creating External Table at Location Root (→ FAIL case) ──")
    EXT_SCHEMA = f"{PREFIX}_ext_schema"
    try:
        w.schemas.create(name=EXT_SCHEMA, catalog_name=CAT_PO_OFF)
        print(f"✓ Created schema: {CAT_PO_OFF}.{EXT_SCHEMA}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"• Schema already exists: {CAT_PO_OFF}.{EXT_SCHEMA}")
        else:
            raise

    ext_table_at_root = f"`{CAT_PO_OFF}`.`{EXT_SCHEMA}`.`table_at_root`"
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ext_table_at_root} (
                id BIGINT, data STRING
            )
            USING DELTA
            LOCATION '{EXT_URL_1}'
        """)
        print(f"✓ Created EXTERNAL table at location root: {ext_table_at_root}")
        print(f"  Location: {EXT_URL_1} (= root of {EXT_LOC_1})")
    except Exception as e:
        print(f"✗ Error creating external table at root: {e}")

    # Also create one in a subdirectory (good practice - not a violation)
    ext_table_subdir = f"`{CAT_PO_OFF}`.`{EXT_SCHEMA}`.`table_in_subdir`"
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ext_table_subdir} (
                id BIGINT, data STRING
            )
            USING DELTA
            LOCATION '{EXT_URL_2}/subdir/my_table'
        """)
        print(f"✓ Created EXTERNAL table in subdirectory: {ext_table_subdir}")
        print(f"  Location: {EXT_URL_2}/subdir/my_table (= subdir of {EXT_LOC_2} ✓)")
    except Exception as e:
        print(f"✗ Error creating external table in subdir: {e}")

    print(f"""
    ┌────────────────────────────────────────────────────────────────┐
    │  ✓ EXTERNAL LOCATIONS SETUP COMPLETE                          │
    │                                                                │
    │  Storage Credential: {CRED_NAME}
    │  External Location 1: {EXT_LOC_1} → {EXT_URL_1}
    │  External Location 2: {EXT_LOC_2} → {EXT_URL_2}
    │                                                                │
    │  Expected: check_storage_credentials    → ❌ FAIL              │
    │            (both locations share '{CRED_NAME}')
    │  Expected: check_external_location_root → ❌ FAIL              │
    │            (table_at_root is at ext location root)             │
    └────────────────────────────────────────────────────────────────┘
    """)
else:
    print("⏭  Skipping external locations setup")
    print("   To enable: fill in adls_container_url + one auth option (3a, 3b, or 3c)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. DBFS Mount
# MAGIC
# MAGIC **Requires:** ADLS URL + Service Principal credentials (options 3b).
# MAGIC
# MAGIC DBFS mounts require direct storage credentials (Service Principal OAuth).
# MAGIC Access Connectors and existing UC credentials **cannot** be used for DBFS mounts
# MAGIC because mounts are a legacy Hadoop-level feature, not a Unity Catalog feature.
# MAGIC
# MAGIC If you're using the Access Connector option, this section will be **skipped** —
# MAGIC that's fine, `check_no_dbfs_mounts` will just check your workspace's existing mounts.

# COMMAND ----------

if SP_CONFIGURED and ADLS_URL:
    MOUNT_POINT = f"/mnt/{PREFIX}_test_mount"

    # Parse storage account name from ADLS URL
    # Expected format: abfss://container@account.dfs.core.windows.net
    import re
    adls_match = re.match(r"abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net(.*)", ADLS_URL)

    if adls_match:
        container = adls_match.group(1)
        storage_account = adls_match.group(2)
        path_suffix = adls_match.group(3) or ""

        # Check if already mounted
        existing_mounts = {m.mountPoint for m in dbutils.fs.mounts()}

        if MOUNT_POINT in existing_mounts:
            print(f"• Mount already exists: {MOUNT_POINT}")
        else:
            print(f"── Creating DBFS Mount ──")
            print(f"  Mount point: {MOUNT_POINT}")
            print(f"  Source:      {ADLS_URL}")
            print(f"  Auth:        OAuth via Service Principal")

            try:
                configs = {
                    "fs.azure.account.auth.type": "OAuth",
                    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                    "fs.azure.account.oauth2.client.id": SP_CLIENT_ID,
                    "fs.azure.account.oauth2.client.secret": SP_CLIENT_SECRET,
                    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{SP_TENANT_ID}/oauth2/token",
                }

                dbutils.fs.mount(
                    source=ADLS_URL,
                    mount_point=MOUNT_POINT,
                    extra_configs=configs,
                )
                print(f"✓ Created DBFS mount: {MOUNT_POINT}")
            except Exception as e:
                print(f"✗ Could not create DBFS mount: {e}")
                print(f"  Common issues:")
                print(f"  - Service Principal doesn't have 'Storage Blob Data Contributor' on the storage account")
                print(f"  - Firewall/network rules on the storage account blocking access")
                print(f"  - DBFS mounts may be disabled on this workspace")

        print(f"""
    ┌───────────────────────────────────────────────────────┐
    │  ✓ DBFS MOUNT SETUP COMPLETE                         │
    │  Mount: {MOUNT_POINT} → {ADLS_URL}
    │  Expected: check_no_dbfs_mounts → ❌ FAIL             │
    └───────────────────────────────────────────────────────┘
        """)
    else:
        print(f"✗ Could not parse ADLS URL: {ADLS_URL}")
        print(f"  Expected format: abfss://container@account.dfs.core.windows.net")
        print(f"  Example:         abfss://data@mystorageaccount.dfs.core.windows.net")
else:
    print("⏭  Skipping DBFS mount setup")
    if AUTH_METHOD == "access_connector":
        print("   DBFS mounts require Service Principal credentials (option 3b).")
        print("   Access Connectors can't be used for legacy DBFS mounts.")
        print("   The check_no_dbfs_mounts check will still work — it checks existing mounts in your workspace.")
    elif AUTH_METHOD == "existing":
        print("   DBFS mounts require Service Principal credentials (option 3b).")
        print("   The check_no_dbfs_mounts check will still work — it checks existing mounts in your workspace.")
    else:
        print("   To enable: fill in adls_container_url + Service Principal credentials (3b)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Data Quality Monitors (optional)
# MAGIC
# MAGIC **Requires:** `warehouse_id` widget (a running SQL Warehouse).
# MAGIC
# MAGIC Creates Lakehouse Monitors on 4 out of 10 tables = 40% → below 50% threshold → **FAIL**.

# COMMAND ----------

if WAREHOUSE_ID:
    MONITOR_OUTPUT_SCHEMA = f"{CAT_PO_ON}.{SCHEMA_NAME}"
    MONITOR_ASSETS_DIR = f"/Shared/{PREFIX}_monitor_assets"
    tables_to_monitor = PO_ON_TABLES[:4]  # Monitor 4 of 7 tables in PO-ON catalog

    # Resolve the monitor API — SDK versions and serverless may differ.
    # Try SDK APIs first, then fall back to REST API.
    monitor_api = None
    use_rest_api = False
    if hasattr(w, 'lakehouse_monitors'):
        monitor_api = w.lakehouse_monitors
        print("Using: w.lakehouse_monitors API")
    elif hasattr(w, 'quality_monitors'):
        monitor_api = w.quality_monitors
        print("Using: w.quality_monitors API (older SDK version)")
    else:
        use_rest_api = True
        print("Using: REST API fallback (SDK monitor APIs not available on this compute)")

    def _create_monitor(table_name, assets_dir, output_schema, warehouse_id):
        """Create a monitor using whatever API is available."""
        if monitor_api:
            monitor_api.create(
                table_name=table_name,
                assets_dir=assets_dir,
                output_schema_name=output_schema,
                snapshot={},
                warehouse_id=warehouse_id,
            )
        else:
            import json
            w.api_client.do(
                'POST',
                f'/api/2.1/unity-catalog/tables/{table_name}/monitor',
                body=json.dumps({
                    "assets_dir": assets_dir,
                    "output_schema_name": output_schema,
                    "snapshot": {},
                    "warehouse_id": warehouse_id,
                }).encode('utf-8'),
            )

    print(f"── Creating Lakehouse Monitors ──")
    print(f"  Warehouse: {WAREHOUSE_ID}")
    print(f"  Output:    {MONITOR_OUTPUT_SCHEMA}")
    print(f"  Assets:    {MONITOR_ASSETS_DIR}")
    print()

    created = 0
    for tbl in tables_to_monitor:
        full_name = f"{CAT_PO_ON}.{SCHEMA_NAME}.{tbl}"
        try:
            _create_monitor(full_name, MONITOR_ASSETS_DIR, MONITOR_OUTPUT_SCHEMA, WAREHOUSE_ID)
            print(f"  ✓ Monitor created: {full_name}")
            created += 1
        except Exception as e:
            if "already" in str(e).lower() or "exists" in str(e).lower():
                print(f"  • Monitor already exists: {full_name}")
                created += 1
            else:
                print(f"  ✗ Error creating monitor for {full_name}: {e}")

    total_count = len(PO_ON_TABLES) + len(PO_OFF_TABLES)
    pct = round(created / total_count * 100, 1) if total_count > 0 else 0
    print(f"""
    ┌─────────────────────────────────────────────────────────┐
    │  ✓ DATA QUALITY MONITORS SETUP COMPLETE                 │
    │  Monitored: {created}/{total_count} tables = {pct}%
    │  Expected: check_data_quality → ❌ FAIL ({pct}% < 50%)
    └─────────────────────────────────────────────────────────┘
    """)
else:
    print("⏭  Skipping data quality monitors")
    print("   To enable: fill in warehouse_id (ID of a running SQL Warehouse)")
    print("   Find it in: SQL Warehouses → your warehouse → Connection Details → HTTP Path → last segment")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Run All 5 Governance Checks
# MAGIC
# MAGIC This is the moment of truth — let's see if our checks detect the assets we created.
# MAGIC
# MAGIC We **scope the checks to only our test catalogs** so they don't scan the entire
# MAGIC enterprise workspace. In the real `governance_analysis.py`, this filter is not set
# MAGIC and all catalogs are scanned.

# COMMAND ----------

import governance_analyzer as ga

# Scope checks to only scan our test catalogs (not the whole workspace)
ga.configure_catalog_filter([CAT_PO_ON, CAT_PO_OFF])

checks_to_test = [
    ("Predictive Optimization", "≥70% managed tables with PO",  ga.check_predictive_optimization),
    ("No DBFS Mounts",          "0 legacy DBFS mounts",         ga.check_no_dbfs_mounts),
    ("External Location Root",  "No objects at ext loc root",   ga.check_external_location_root),
    ("Storage Credentials",     "Independent creds per ext loc", ga.check_storage_credentials),
    ("Data Quality",            "≥50% tables with monitoring",  ga.check_data_quality),
]

print("=" * 80)
print("  GOVERNANCE CHECK RESULTS")
print("=" * 80)

for name, description, fn in checks_to_test:
    result = fn()
    status = result["status"].upper()
    icon = {"PASS": "✅", "FAIL": "❌", "WARNING": "⚠️ ", "ERROR": "🔴"}.get(status, "❓")

    print(f"\n{icon} [{status:7s}]  {name}")
    print(f"   Rule:    {description}")
    print(f"   Score:   {result['score']}/{result['max_score']}")
    print(f"   Details: {result['details']}")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Cleanup — Remove All Test Assets
# MAGIC
# MAGIC **⚠️  Uncomment the code below and run it to delete everything this notebook created.**

# COMMAND ----------

# ┌─────────────────────────────────────────────────────────────────────────┐
# │  UNCOMMENT THIS ENTIRE BLOCK TO CLEAN UP ALL TEST ASSETS              │
# └─────────────────────────────────────────────────────────────────────────┘

# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()

# PREFIX = dbutils.widgets.get("prefix")
# CAT_PO_ON = f"{PREFIX}_po_on"
# CAT_PO_OFF = f"{PREFIX}_po_off"
# SCHEMA_NAME = "test_data"
# PO_ON_TABLES = [f"managed_tbl_{i}" for i in range(1, 8)]
# CRED_NAME = f"{PREFIX}_credential"

# print("── Cleaning up test assets ──\n")

# # 1. Delete Lakehouse Monitors
# monitor_api = getattr(w, 'lakehouse_monitors', None) or getattr(w, 'quality_monitors', None)
# if monitor_api:
#     for tbl in PO_ON_TABLES[:4]:
#         full_name = f"{CAT_PO_ON}.{SCHEMA_NAME}.{tbl}"
#         try:
#             monitor_api.delete(table_name=full_name)
#             print(f"  ✓ Deleted monitor: {full_name}")
#         except Exception:
#             pass

# # 2. Delete external locations
# for loc_name in [f"{PREFIX}_ext_loc_1", f"{PREFIX}_ext_loc_2"]:
#     try:
#         w.external_locations.delete(name=loc_name, force=True)
#         print(f"  ✓ Deleted external location: {loc_name}")
#     except Exception:
#         pass

# # 3. Delete storage credential (only if we created it, not if using an existing one)
# EXISTING_CREDENTIAL = dbutils.widgets.get("existing_credential_name").strip() or None
# if not EXISTING_CREDENTIAL:
#     try:
#         w.storage_credentials.delete(name=CRED_NAME, force=True)
#         print(f"  ✓ Deleted storage credential: {CRED_NAME}")
#     except Exception:
#         pass
# else:
#     print(f"  • Skipping credential deletion (using existing: {EXISTING_CREDENTIAL})")

# # 4. Unmount DBFS
# try:
#     dbutils.fs.unmount(f"/mnt/{PREFIX}_test_mount")
#     print(f"  ✓ Unmounted: /mnt/{PREFIX}_test_mount")
# except Exception:
#     pass

# # 5. Drop catalogs (CASCADE drops all schemas, tables, volumes inside)
# for cat_name in [CAT_PO_ON, CAT_PO_OFF]:
#     try:
#         spark.sql(f"DROP CATALOG IF EXISTS `{cat_name}` CASCADE")
#         print(f"  ✓ Dropped catalog: {cat_name}")
#     except Exception as e:
#         print(f"  ✗ Error dropping {cat_name}: {e}")

# # 6. Reset catalog filter
# import governance_analyzer as ga
# ga.configure_catalog_filter(None)

# print("\n✓ All test assets cleaned up!")
