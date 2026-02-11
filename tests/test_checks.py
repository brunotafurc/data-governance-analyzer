"""Unit tests for governance check functions.

These tests mock the Databricks SDK to validate check logic without
requiring a live Databricks workspace connection.

Run with:  python -m pytest tests/ -v
"""

import sys
from unittest.mock import MagicMock, patch, PropertyMock
from types import SimpleNamespace

import pytest

# ---------------------------------------------------------------------------
# Helpers to build mock SDK objects
# ---------------------------------------------------------------------------

def _make_catalog(name, po_setting=None, catalog_type=None):
    """Create a mock CatalogInfo."""
    cat = SimpleNamespace(
        name=name,
        enable_predictive_optimization=po_setting,
        catalog_type=catalog_type,
    )
    return cat


def _make_schema(name, po_setting=None):
    """Create a mock SchemaInfo."""
    return SimpleNamespace(
        name=name,
        enable_predictive_optimization=po_setting,
    )


def _make_table(name, table_type="MANAGED", storage_location=None):
    """Create a mock TableInfo."""
    return SimpleNamespace(
        name=name,
        table_type=table_type,
        storage_location=storage_location,
    )


def _make_volume(name, volume_type="MANAGED", storage_location=None):
    """Create a mock VolumeInfo."""
    return SimpleNamespace(
        name=name,
        volume_type=volume_type,
        storage_location=storage_location,
    )


def _make_ext_location(name, url, credential_name):
    """Create a mock ExternalLocationInfo."""
    return SimpleNamespace(
        name=name,
        url=url,
        credential_name=credential_name,
    )


def _make_workspace_client(catalogs=None, schemas_map=None, tables_map=None,
                            volumes_map=None, ext_locations=None,
                            lakehouse_monitors_map=None):
    """
    Build a fully-wired mock WorkspaceClient.

    Args:
        catalogs: list of CatalogInfo mocks
        schemas_map: dict of catalog_name -> list of SchemaInfo mocks
        tables_map: dict of "catalog.schema" -> list of TableInfo mocks
        volumes_map: dict of "catalog.schema" -> list of VolumeInfo mocks
        ext_locations: list of ExternalLocationInfo mocks
        lakehouse_monitors_map: dict of full_table_name -> MonitorInfo (or None to raise)
    """
    w = MagicMock()

    # catalogs.list()
    w.catalogs.list.return_value = catalogs or []

    # schemas.list(catalog_name=...)
    _schemas = schemas_map or {}
    def _schema_list(catalog_name):
        return _schemas.get(catalog_name, [])
    w.schemas.list.side_effect = _schema_list

    # tables.list(catalog_name=..., schema_name=...)
    _tables = tables_map or {}
    def _table_list(catalog_name, schema_name):
        return _tables.get(f"{catalog_name}.{schema_name}", [])
    w.tables.list.side_effect = _table_list

    # volumes.list(catalog_name=..., schema_name=...)
    _volumes = volumes_map or {}
    def _volume_list(catalog_name, schema_name):
        return _volumes.get(f"{catalog_name}.{schema_name}", [])
    w.volumes.list.side_effect = _volume_list

    # external_locations.list()
    w.external_locations.list.return_value = ext_locations or []

    # lakehouse_monitors.get(table_name=...)
    _monitors = lakehouse_monitors_map or {}
    def _monitor_get(table_name):
        if table_name in _monitors:
            return _monitors[table_name]
        raise Exception(f"RESOURCE_DOES_NOT_EXIST: Monitor for table '{table_name}' not found")
    w.lakehouse_monitors.get.side_effect = _monitor_get

    return w


# ---------------------------------------------------------------------------
# Tests: _resolve_po_status
# ---------------------------------------------------------------------------

class TestResolvePOStatus:
    """Tests for the _resolve_po_status helper."""

    def test_schema_enable_overrides_catalog(self):
        from governance_analyzer.checks import _resolve_po_status
        assert _resolve_po_status("ENABLE", "DISABLE") is True

    def test_schema_disable_overrides_catalog(self):
        from governance_analyzer.checks import _resolve_po_status
        assert _resolve_po_status("DISABLE", "ENABLE") is False

    def test_schema_inherit_falls_to_catalog_enable(self):
        from governance_analyzer.checks import _resolve_po_status
        assert _resolve_po_status("INHERIT", "ENABLE") is True

    def test_schema_inherit_falls_to_catalog_disable(self):
        from governance_analyzer.checks import _resolve_po_status
        assert _resolve_po_status("INHERIT", "DISABLE") is False

    def test_both_none_returns_false(self):
        from governance_analyzer.checks import _resolve_po_status
        assert _resolve_po_status(None, None) is False

    def test_schema_none_catalog_enable(self):
        from governance_analyzer.checks import _resolve_po_status
        assert _resolve_po_status(None, "ENABLE") is True

    def test_handles_enum_style_string(self):
        """SDK may return 'EnablePredictiveOptimization.ENABLE' style strings."""
        from governance_analyzer.checks import _resolve_po_status
        assert _resolve_po_status("EnablePredictiveOptimization.INHERIT",
                                   "EnablePredictiveOptimization.ENABLE") is True
        assert _resolve_po_status("EnablePredictiveOptimization.DISABLE",
                                   "EnablePredictiveOptimization.ENABLE") is False


# ---------------------------------------------------------------------------
# Tests: check_storage_credentials
# ---------------------------------------------------------------------------

class TestCheckStorageCredentials:

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_pass_when_no_external_locations(self, mock_gwc):
        from governance_analyzer.checks import check_storage_credentials
        mock_gwc.return_value = _make_workspace_client(ext_locations=[])
        result = check_storage_credentials()
        assert result["status"] == "pass"

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_pass_when_all_independent_credentials(self, mock_gwc):
        from governance_analyzer.checks import check_storage_credentials
        locs = [
            _make_ext_location("loc1", "s3://bucket1/", "cred_A"),
            _make_ext_location("loc2", "s3://bucket2/", "cred_B"),
            _make_ext_location("loc3", "s3://bucket3/", "cred_C"),
        ]
        mock_gwc.return_value = _make_workspace_client(ext_locations=locs)
        result = check_storage_credentials()
        assert result["status"] == "pass"
        assert result["score"] == 1

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_fail_when_credentials_shared(self, mock_gwc):
        from governance_analyzer.checks import check_storage_credentials
        locs = [
            _make_ext_location("loc1", "s3://bucket1/", "shared_cred"),
            _make_ext_location("loc2", "s3://bucket2/", "shared_cred"),
            _make_ext_location("loc3", "s3://bucket3/", "cred_C"),
        ]
        mock_gwc.return_value = _make_workspace_client(ext_locations=locs)
        result = check_storage_credentials()
        assert result["status"] == "fail"
        assert result["score"] == 0
        assert "shared_cred" in result["details"]


# ---------------------------------------------------------------------------
# Tests: check_no_dbfs_mounts
# ---------------------------------------------------------------------------

class TestCheckNoDbfsMounts:

    def test_pass_when_no_custom_mounts(self):
        from governance_analyzer.checks import check_no_dbfs_mounts

        # Simulate dbutils.fs.mounts() returning only system mounts
        mock_mount_root = SimpleNamespace(mountPoint="/", source="root")
        mock_mount_datasets = SimpleNamespace(mountPoint="/databricks-datasets", source="datasets")

        import builtins
        mock_dbutils = MagicMock()
        mock_dbutils.fs.mounts.return_value = [mock_mount_root, mock_mount_datasets]
        builtins.dbutils = mock_dbutils
        try:
            result = check_no_dbfs_mounts()
            assert result["status"] == "pass"
            assert result["score"] == 3
        finally:
            del builtins.dbutils

    def test_fail_when_custom_mounts_exist(self):
        from governance_analyzer.checks import check_no_dbfs_mounts

        mock_mounts = [
            SimpleNamespace(mountPoint="/", source="root"),
            SimpleNamespace(mountPoint="/mnt/datalake", source="wasbs://container@account.blob.core.windows.net/"),
            SimpleNamespace(mountPoint="/mnt/raw", source="s3a://my-bucket/raw"),
        ]

        import builtins
        mock_dbutils = MagicMock()
        mock_dbutils.fs.mounts.return_value = mock_mounts
        builtins.dbutils = mock_dbutils
        try:
            result = check_no_dbfs_mounts()
            assert result["status"] == "fail"
            assert result["score"] == 0
            assert "2 DBFS mount(s)" in result["details"]
        finally:
            del builtins.dbutils

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_fallback_to_sdk_when_dbutils_missing(self, mock_gwc):
        from governance_analyzer.checks import check_no_dbfs_mounts

        # Ensure dbutils is NOT in builtins
        import builtins
        if hasattr(builtins, 'dbutils'):
            del builtins.dbutils

        w = MagicMock()
        w.dbfs.list.return_value = []  # No entries under /mnt
        mock_gwc.return_value = w

        result = check_no_dbfs_mounts()
        assert result["status"] == "pass"


# ---------------------------------------------------------------------------
# Tests: check_external_location_root
# ---------------------------------------------------------------------------

class TestCheckExternalLocationRoot:

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_pass_when_no_external_locations(self, mock_gwc):
        from governance_analyzer.checks import check_external_location_root
        mock_gwc.return_value = _make_workspace_client(ext_locations=[])
        result = check_external_location_root()
        assert result["status"] == "pass"

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_pass_when_tables_in_subdirectories(self, mock_gwc):
        from governance_analyzer.checks import check_external_location_root

        ext_locs = [_make_ext_location("loc1", "s3://bucket/path/", "cred1")]
        catalogs = [_make_catalog("my_catalog")]
        schemas = {"my_catalog": [_make_schema("my_schema")]}
        tables = {
            "my_catalog.my_schema": [
                _make_table("ext_table", "EXTERNAL", "s3://bucket/path/subdir/data"),
            ]
        }

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
            ext_locations=ext_locs,
        )
        result = check_external_location_root()
        assert result["status"] == "pass"

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_fail_when_table_at_root(self, mock_gwc):
        from governance_analyzer.checks import check_external_location_root

        ext_locs = [_make_ext_location("loc1", "s3://bucket/path/", "cred1")]
        catalogs = [_make_catalog("my_catalog")]
        schemas = {"my_catalog": [_make_schema("my_schema")]}
        # Table storage_location matches ext location root exactly
        tables = {
            "my_catalog.my_schema": [
                _make_table("bad_table", "EXTERNAL", "s3://bucket/path/"),
            ]
        }

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
            ext_locations=ext_locs,
        )
        result = check_external_location_root()
        assert result["status"] == "fail"
        assert "bad_table" in result["details"]

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_fail_when_volume_at_root(self, mock_gwc):
        from governance_analyzer.checks import check_external_location_root

        ext_locs = [_make_ext_location("loc1", "s3://bucket/path", "cred1")]
        catalogs = [_make_catalog("my_catalog")]
        schemas = {"my_catalog": [_make_schema("my_schema")]}
        volumes = {
            "my_catalog.my_schema": [
                _make_volume("bad_vol", "EXTERNAL", "s3://bucket/path/"),
            ]
        }

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas,
            tables_map={}, volumes_map=volumes,
            ext_locations=ext_locs,
        )
        result = check_external_location_root()
        assert result["status"] == "fail"
        assert "bad_vol" in result["details"]


# ---------------------------------------------------------------------------
# Tests: check_predictive_optimization
# ---------------------------------------------------------------------------

class TestCheckPredictiveOptimization:

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_pass_when_above_threshold(self, mock_gwc):
        from governance_analyzer.checks import check_predictive_optimization

        catalogs = [_make_catalog("prod", po_setting="ENABLE")]
        schemas = {"prod": [_make_schema("bronze")]}
        tables = {
            "prod.bronze": [
                _make_table("t1", "MANAGED"),
                _make_table("t2", "MANAGED"),
                _make_table("t3", "MANAGED"),
            ]
        }

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
        )
        result = check_predictive_optimization()
        assert result["status"] == "pass"
        assert "100.0%" in result["details"]

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_fail_when_below_threshold(self, mock_gwc):
        from governance_analyzer.checks import check_predictive_optimization

        catalogs = [
            _make_catalog("prod", po_setting="ENABLE"),
            _make_catalog("dev", po_setting="DISABLE"),
        ]
        schemas = {
            "prod": [_make_schema("data")],
            "dev": [_make_schema("data")],
        }
        tables = {
            # 2 managed in PO-enabled catalog
            "prod.data": [_make_table("t1", "MANAGED"), _make_table("t2", "MANAGED")],
            # 8 managed in PO-disabled catalog
            "dev.data": [_make_table(f"t{i}", "MANAGED") for i in range(3, 11)],
        }

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
        )
        result = check_predictive_optimization()
        assert result["status"] == "fail"
        assert result["score"] == 0
        # 2/10 = 20%, below 70% threshold
        assert "20.0%" in result["details"]

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_schema_override_catalog(self, mock_gwc):
        from governance_analyzer.checks import check_predictive_optimization

        # Catalog has PO disabled, but schema overrides to ENABLE
        catalogs = [_make_catalog("prod", po_setting="DISABLE")]
        schemas = {"prod": [_make_schema("optimized", po_setting="ENABLE")]}
        tables = {"prod.optimized": [_make_table("t1", "MANAGED")]}

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
        )
        result = check_predictive_optimization()
        assert result["status"] == "pass"
        assert result["score"] == 1

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_skips_external_tables(self, mock_gwc):
        from governance_analyzer.checks import check_predictive_optimization

        catalogs = [_make_catalog("prod", po_setting="DISABLE")]
        schemas = {"prod": [_make_schema("data")]}
        tables = {
            "prod.data": [
                _make_table("ext_t", "EXTERNAL"),
                _make_table("view_t", "VIEW"),
            ]
        }

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
        )
        result = check_predictive_optimization()
        assert result["status"] == "warning"  # No managed tables found

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_skips_system_catalogs(self, mock_gwc):
        from governance_analyzer.checks import check_predictive_optimization

        catalogs = [
            _make_catalog("system"),
            _make_catalog("hive_metastore"),
            _make_catalog("prod", po_setting="ENABLE"),
        ]
        schemas = {
            "system": [_make_schema("access")],
            "hive_metastore": [_make_schema("default")],
            "prod": [_make_schema("data")],
        }
        tables = {
            "system.access": [_make_table("sys_t", "MANAGED")],
            "hive_metastore.default": [_make_table("hive_t", "MANAGED")],
            "prod.data": [_make_table("t1", "MANAGED")],
        }

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
        )
        result = check_predictive_optimization()
        assert result["status"] == "pass"
        # Only prod.data.t1 should be counted (1/1 = 100%)
        assert "1/1" in result["details"]


# ---------------------------------------------------------------------------
# Tests: check_data_quality
# ---------------------------------------------------------------------------

class TestCheckDataQuality:

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_pass_when_above_threshold(self, mock_gwc):
        from governance_analyzer.checks import check_data_quality

        catalogs = [_make_catalog("prod")]
        schemas = {"prod": [_make_schema("data")]}
        tables = {
            "prod.data": [
                _make_table("t1", "MANAGED"),
                _make_table("t2", "MANAGED"),
            ]
        }
        monitors = {
            "prod.data.t1": SimpleNamespace(status="ACTIVE"),
            "prod.data.t2": SimpleNamespace(status="ACTIVE"),
        }

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
            lakehouse_monitors_map=monitors,
        )
        result = check_data_quality()
        assert result["status"] == "pass"
        assert result["score"] == 1

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_fail_when_below_threshold(self, mock_gwc):
        from governance_analyzer.checks import check_data_quality

        catalogs = [_make_catalog("prod")]
        schemas = {"prod": [_make_schema("data")]}
        tables = {
            "prod.data": [
                _make_table("t1", "MANAGED"),
                _make_table("t2", "MANAGED"),
                _make_table("t3", "MANAGED"),
                _make_table("t4", "MANAGED"),
            ]
        }
        # Only 1 out of 4 monitored = 25%
        monitors = {
            "prod.data.t1": SimpleNamespace(status="ACTIVE"),
        }

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
            lakehouse_monitors_map=monitors,
        )
        result = check_data_quality()
        assert result["status"] == "fail"
        assert result["score"] == 0
        assert "25.0%" in result["details"]

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_warning_when_no_tables(self, mock_gwc):
        from governance_analyzer.checks import check_data_quality

        mock_gwc.return_value = _make_workspace_client(
            catalogs=[], schemas_map={}, tables_map={},
        )
        result = check_data_quality()
        assert result["status"] == "warning"

    @patch("governance_analyzer.checks.get_workspace_client")
    def test_skips_views(self, mock_gwc):
        from governance_analyzer.checks import check_data_quality

        catalogs = [_make_catalog("prod")]
        schemas = {"prod": [_make_schema("data")]}
        tables = {
            "prod.data": [
                _make_table("v1", "VIEW"),
                _make_table("t1", "MANAGED"),
            ]
        }
        monitors = {"prod.data.t1": SimpleNamespace(status="ACTIVE")}

        mock_gwc.return_value = _make_workspace_client(
            catalogs=catalogs, schemas_map=schemas, tables_map=tables,
            lakehouse_monitors_map=monitors,
        )
        result = check_data_quality()
        assert result["status"] == "pass"
        # Only t1 counted, view skipped → 1/1 = 100%
        assert "1/1" in result["details"]
