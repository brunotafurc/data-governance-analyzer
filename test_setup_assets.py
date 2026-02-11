# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Checks — Test Asset Setup
# MAGIC
# MAGIC This notebook creates real Databricks assets to validate the 5 governance checks:
# MAGIC
# MAGIC | Check | What it tests | Assets created |
# MAGIC |-------|---------------|----------------|
# MAGIC | **Predictive Optimization** | ≥ 70% managed tables with PO | 2 catalogs (PO on/off), schemas, managed tables |
# MAGIC | **DBFS Mounts** | 0 legacy mounts | 1 DBFS mount (optional, needs cloud storage) |
# MAGIC | **External Location Root** | No objects at ext location root | External location + table at root (optional) |
# MAGIC | **Storage Credentials** | Independent credentials per location | 2 ext locations sharing 1 credential (optional) |
# MAGIC | **Data Quality** | ≥ 50% tables with monitoring | Lakehouse monitors on some tables (optional) |
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. **Core setup** (always runs) — catalogs, schemas, managed tables, PO config
# MAGIC 2. **External locations** (optional) — needs a cloud storage path + credential
# MAGIC 3. **DBFS mount** (optional) — needs a cloud storage path
# MAGIC 4. **Data quality monitors** (optional) — needs a SQL warehouse
# MAGIC 5. **Run the checks** — execute and inspect results
# MAGIC 6. **Cleanup** — tear down all test assets

# COMMAND ----------

%pip install databricks-sdk --upgrade --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Fill in the widgets below. Only the **prefix** is required.
# MAGIC The cloud-specific fields are optional — leave them empty to skip those sections.

# COMMAND ----------

dbutils.widgets.text("prefix", "gov_test", "Resource prefix")
dbutils.widgets.text("storage_credential_name", "", "Existing storage credential name (optional)")
dbutils.widgets.text("cloud_storage_url", "", "Cloud storage URL, e.g. s3://bucket/path or abfss://container@account.dfs.core.windows.net/path (optional)")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID for Lakehouse Monitors (optional)")

PREFIX = dbutils.widgets.get("prefix")
STORAGE_CREDENTIAL = dbutils.widgets.get("storage_credential_name").strip() or None
CLOUD_STORAGE_URL = dbutils.widgets.get("cloud_storage_url").strip().rstrip("/") or None
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip() or None

print(f"Prefix:             {PREFIX}")
print(f"Storage credential: {STORAGE_CREDENTIAL or '(skipped — external location tests will not run)'}")
print(f"Cloud storage URL:  {CLOUD_STORAGE_URL or '(skipped — mount + ext location tests will not run)'}")
print(f"Warehouse ID:       {WAREHOUSE_ID or '(skipped — data quality monitor tests will not run)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Core Setup — Catalogs, Schemas, Tables, PO

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Catalog names
CAT_PO_ON = f"{PREFIX}_po_on"
CAT_PO_OFF = f"{PREFIX}_po_off"

# -- Create catalogs -------------------------------------------------------
for cat_name in [CAT_PO_ON, CAT_PO_OFF]:
    try:
        w.catalogs.create(name=cat_name)
        print(f"✓ Created catalog: {cat_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"• Catalog already exists: {cat_name}")
        else:
            raise

# COMMAND ----------

# -- Enable / disable predictive optimization on catalogs -------------------
from databricks.sdk.service.catalog import EnablePredictiveOptimization

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

# COMMAND ----------

# -- Create schemas ---------------------------------------------------------
SCHEMA_NAME = "test_data"

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

# -- Create managed tables --------------------------------------------------
# We create 7 tables in the PO-ON catalog and 3 in the PO-OFF catalog.
# That gives 7/10 = 70% with PO → exactly at the pass threshold.

PO_ON_TABLES = [f"managed_tbl_{i}" for i in range(1, 8)]   # 7 tables
PO_OFF_TABLES = [f"managed_tbl_{i}" for i in range(1, 4)]  # 3 tables

for cat_name, table_list in [(CAT_PO_ON, PO_ON_TABLES), (CAT_PO_OFF, PO_OFF_TABLES)]:
    for tbl in table_list:
        full_name = f"{cat_name}.{SCHEMA_NAME}.{tbl}"
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_name} (
                    id BIGINT,
                    name STRING,
                    value DOUBLE,
                    created_at TIMESTAMP
                )
            """)
            # Insert a few rows so tables are not empty (needed for monitors later)
            spark.sql(f"""
                INSERT INTO {full_name} VALUES
                (1, 'alpha', 10.5, current_timestamp()),
                (2, 'beta', 20.3, current_timestamp()),
                (3, 'gamma', 30.1, current_timestamp())
            """)
            print(f"✓ Created + populated: {full_name}")
        except Exception as e:
            print(f"✗ Error creating {full_name}: {e}")

# COMMAND ----------

# -- Summary ----------------------------------------------------------------
total = len(PO_ON_TABLES) + len(PO_OFF_TABLES)
pct = round(len(PO_ON_TABLES) / total * 100, 1)

print(f"""
╔══════════════════════════════════════════════════════════╗
║  CORE SETUP COMPLETE                                    ║
╠══════════════════════════════════════════════════════════╣
║  Catalogs:   {CAT_PO_ON} (PO=ON), {CAT_PO_OFF} (PO=OFF)
║  Schema:     {SCHEMA_NAME}
║  Tables:     {len(PO_ON_TABLES)} in PO-ON + {len(PO_OFF_TABLES)} in PO-OFF = {total} total
║  Expected:   PO check → {pct}% (threshold: 70%) → PASS
╚══════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. External Locations (optional)
# MAGIC
# MAGIC **Requires:** `storage_credential_name` and `cloud_storage_url` widgets filled in.
# MAGIC
# MAGIC Creates **two external locations sharing the same credential** (→ `check_storage_credentials` will FAIL),
# MAGIC and one **external table at the location root** (→ `check_external_location_root` will FAIL).

# COMMAND ----------

if STORAGE_CREDENTIAL and CLOUD_STORAGE_URL:
    EXT_LOC_1 = f"{PREFIX}_ext_loc_1"
    EXT_LOC_2 = f"{PREFIX}_ext_loc_2"
    EXT_URL_1 = f"{CLOUD_STORAGE_URL}/{PREFIX}_loc1"
    EXT_URL_2 = f"{CLOUD_STORAGE_URL}/{PREFIX}_loc2"

    # -- Create two external locations with the SAME credential (fail case) --
    for loc_name, loc_url in [(EXT_LOC_1, EXT_URL_1), (EXT_LOC_2, EXT_URL_2)]:
        try:
            w.external_locations.create(
                name=loc_name,
                url=loc_url,
                credential_name=STORAGE_CREDENTIAL,
                skip_validation=True,
            )
            print(f"✓ Created external location: {loc_name} → {loc_url} (credential: {STORAGE_CREDENTIAL})")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"• External location already exists: {loc_name}")
            else:
                print(f"✗ Error creating {loc_name}: {e}")

    # -- Create an external table AT the root of ext location 1 (fail case) --
    EXT_SCHEMA = f"{PREFIX}_ext_schema"
    try:
        w.schemas.create(name=EXT_SCHEMA, catalog_name=CAT_PO_OFF)
        print(f"✓ Created schema: {CAT_PO_OFF}.{EXT_SCHEMA}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"• Schema already exists: {CAT_PO_OFF}.{EXT_SCHEMA}")
        else:
            raise

    ext_table_at_root = f"{CAT_PO_OFF}.{EXT_SCHEMA}.table_at_root"
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ext_table_at_root} (
                id BIGINT, data STRING
            )
            USING DELTA
            LOCATION '{EXT_URL_1}'
        """)
        print(f"✓ Created external table AT root: {ext_table_at_root} → {EXT_URL_1}")
    except Exception as e:
        print(f"✗ Error creating external table at root: {e}")

    ext_table_subdir = f"{CAT_PO_OFF}.{EXT_SCHEMA}.table_in_subdir"
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ext_table_subdir} (
                id BIGINT, data STRING
            )
            USING DELTA
            LOCATION '{EXT_URL_2}/subdir/my_table'
        """)
        print(f"✓ Created external table in subdir: {ext_table_subdir} → {EXT_URL_2}/subdir/my_table")
    except Exception as e:
        print(f"✗ Error creating external table in subdir: {e}")

    print(f"""
    ┌─────────────────────────────────────────────────────────┐
    │  EXTERNAL LOCATIONS SETUP COMPLETE                      │
    │  Expected: check_storage_credentials    → FAIL          │
    │            (both locations share '{STORAGE_CREDENTIAL}') │
    │  Expected: check_external_location_root → FAIL          │
    │            (table_at_root sits at ext location root)     │
    └─────────────────────────────────────────────────────────┘
    """)
else:
    print("⏭ Skipping external locations setup (no storage_credential_name / cloud_storage_url provided)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. DBFS Mount (optional)
# MAGIC
# MAGIC **Requires:** `cloud_storage_url` widget filled in, and the cluster must have
# MAGIC access to the storage (via instance profile, cluster credential, etc.).
# MAGIC
# MAGIC Creates a single DBFS mount → `check_no_dbfs_mounts` will FAIL.

# COMMAND ----------

if CLOUD_STORAGE_URL:
    MOUNT_POINT = f"/mnt/{PREFIX}_test_mount"

    # Check if already mounted
    existing_mounts = {m.mountPoint for m in dbutils.fs.mounts()}
    if MOUNT_POINT in existing_mounts:
        print(f"• Mount already exists: {MOUNT_POINT}")
    else:
        try:
            dbutils.fs.mount(
                source=CLOUD_STORAGE_URL,
                mount_point=MOUNT_POINT,
            )
            print(f"✓ Created DBFS mount: {MOUNT_POINT} → {CLOUD_STORAGE_URL}")
        except Exception as e:
            print(f"✗ Could not create DBFS mount (may need storage access configured on cluster): {e}")
            print(f"  Tip: The mount is optional. check_no_dbfs_mounts will still work — it will")
            print(f"  just report existing mounts or pass if there are none.")

    print(f"""
    ┌───────────────────────────────────────────────┐
    │  DBFS MOUNT SETUP COMPLETE                    │
    │  Expected: check_no_dbfs_mounts → FAIL        │
    │            (found mount at {MOUNT_POINT})      │
    └───────────────────────────────────────────────┘
    """)
else:
    print("⏭ Skipping DBFS mount setup (no cloud_storage_url provided)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Data Quality Monitors (optional)
# MAGIC
# MAGIC **Requires:** `warehouse_id` widget filled in (a running SQL warehouse).
# MAGIC
# MAGIC Creates Lakehouse Monitors on **some** tables in the PO-ON catalog.
# MAGIC We monitor 4 out of 7 tables = 57% → above the 50% threshold for a PASS
# MAGIC (when looking at only that catalog; overall depends on all catalogs).

# COMMAND ----------

if WAREHOUSE_ID:
    MONITOR_OUTPUT_SCHEMA = f"{CAT_PO_ON}.{SCHEMA_NAME}"
    MONITOR_ASSETS_DIR = f"/Shared/{PREFIX}_monitor_assets"
    tables_to_monitor = PO_ON_TABLES[:4]  # Monitor first 4 of 7

    for tbl in tables_to_monitor:
        full_name = f"{CAT_PO_ON}.{SCHEMA_NAME}.{tbl}"
        try:
            w.lakehouse_monitors.create(
                table_name=full_name,
                assets_dir=MONITOR_ASSETS_DIR,
                output_schema_name=MONITOR_OUTPUT_SCHEMA,
                snapshot={},  # Snapshot-based monitoring (simplest)
                warehouse_id=WAREHOUSE_ID,
            )
            print(f"✓ Created monitor: {full_name}")
        except Exception as e:
            if "already" in str(e).lower() or "exists" in str(e).lower():
                print(f"• Monitor already exists: {full_name}")
            else:
                print(f"✗ Error creating monitor for {full_name}: {e}")

    monitored_count = len(tables_to_monitor)
    total_count = len(PO_ON_TABLES) + len(PO_OFF_TABLES)
    pct = round(monitored_count / total_count * 100, 1)
    print(f"""
    ┌─────────────────────────────────────────────────────┐
    │  DATA QUALITY MONITORS SETUP COMPLETE               │
    │  Monitored: {monitored_count}/{total_count} tables = {pct}%                        │
    │  Expected: check_data_quality → FAIL                │
    │            (40% < 50% threshold)                    │
    │  Tip: Monitor more tables to make it PASS           │
    └─────────────────────────────────────────────────────┘
    """)
else:
    print("⏭ Skipping data quality monitors (no warehouse_id provided)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Run the Governance Checks

# COMMAND ----------

import governance_analyzer as ga

checks_to_test = [
    ("Predictive Optimization", ga.check_predictive_optimization),
    ("No DBFS Mounts",          ga.check_no_dbfs_mounts),
    ("External Location Root",  ga.check_external_location_root),
    ("Storage Credentials",     ga.check_storage_credentials),
    ("Data Quality",            ga.check_data_quality),
]

print("=" * 80)
print("  GOVERNANCE CHECK RESULTS")
print("=" * 80)

for name, fn in checks_to_test:
    result = fn()
    status = result["status"].upper()
    icon = {"PASS": "✅", "FAIL": "❌", "WARNING": "⚠️", "ERROR": "🔴"}.get(status, "❓")
    print(f"\n{icon}  [{status:7s}]  {name}")
    print(f"   Score: {result['score']}/{result['max_score']}")
    print(f"   Details: {result['details']}")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Cleanup
# MAGIC
# MAGIC **⚠️ Uncomment the cell below to delete ALL test assets.**
# MAGIC
# MAGIC Run this when you're done testing. It drops catalogs (with CASCADE),
# MAGIC removes external locations, DBFS mounts, and Lakehouse monitors.

# COMMAND ----------

# ──────────────────────────────────────────────────────────────
# UNCOMMENT THIS ENTIRE BLOCK TO CLEAN UP
# ──────────────────────────────────────────────────────────────

# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()

# PREFIX = dbutils.widgets.get("prefix")
# CAT_PO_ON = f"{PREFIX}_po_on"
# CAT_PO_OFF = f"{PREFIX}_po_off"
# SCHEMA_NAME = "test_data"
# PO_ON_TABLES = [f"managed_tbl_{i}" for i in range(1, 8)]

# # 1. Delete Lakehouse Monitors
# for tbl in PO_ON_TABLES[:4]:
#     full_name = f"{CAT_PO_ON}.{SCHEMA_NAME}.{tbl}"
#     try:
#         w.lakehouse_monitors.delete(table_name=full_name)
#         print(f"✓ Deleted monitor: {full_name}")
#     except Exception:
#         pass

# # 2. Delete external locations
# for loc_name in [f"{PREFIX}_ext_loc_1", f"{PREFIX}_ext_loc_2"]:
#     try:
#         w.external_locations.delete(name=loc_name, force=True)
#         print(f"✓ Deleted external location: {loc_name}")
#     except Exception:
#         pass

# # 3. Unmount DBFS
# MOUNT_POINT = f"/mnt/{PREFIX}_test_mount"
# try:
#     dbutils.fs.unmount(MOUNT_POINT)
#     print(f"✓ Unmounted: {MOUNT_POINT}")
# except Exception:
#     pass

# # 4. Drop catalogs (CASCADE drops all schemas and tables inside)
# for cat_name in [CAT_PO_ON, CAT_PO_OFF]:
#     try:
#         spark.sql(f"DROP CATALOG IF EXISTS {cat_name} CASCADE")
#         print(f"✓ Dropped catalog: {cat_name}")
#     except Exception as e:
#         print(f"✗ Error dropping {cat_name}: {e}")

# print("\n✓ Cleanup complete!")
