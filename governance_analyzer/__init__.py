"""Data Governance Analyzer for Databricks Unity Catalog."""

from .clients import (
    get_workspace_client,
    get_account_client,
    get_workspace_region,
    configure_account_auth,
)

from .checks import (
    check_metastore_connected,
    check_metastore_region,
    check_scim_aim_provisioning,
    check_account_admin_group,
    check_metastore_admin_group,
    check_workspace_admin_group,
    check_catalog_admin_group,
    check_at_least_one_account_admin,
    check_account_admin_percentage,
    check_multiple_catalogs,
    check_catalog_binding,
    check_managed_tables_percentage,
    check_no_external_storage,
    check_uc_compute,
    check_no_hive_data,
    check_hive_disabled,
    check_system_tables,
    check_service_principals,
    check_production_access,
    check_group_ownership,
    check_predictive_optimization,
    check_no_dbfs_mounts,
    check_external_location_root,
    check_storage_credentials,
    check_data_quality,
)

from .dashboard import create_dashboard
