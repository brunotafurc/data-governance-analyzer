# Data Governance Analyzer for Databricks

Evaluates Unity Catalog governance best practices across 25 checks in 6 categories.

## Setup

1. Import this project to your Databricks workspace
2. Run the `governance_analysis.ipynb` notebook with optional parameters:
   - `catalog_name` (default: `main`) - Target catalog for results table
   - `schema_name` (default: `default`) - Target schema for results table

## Usage

### Interactive Mode
When running the notebook interactively, widgets will appear at the top:
- Set `catalog_name` to your target catalog (default: `main`)
- Set `schema_name` to your target schema (default: `default`)

### Programmatic/Job Mode
Pass parameters when running as a job:
```python
dbutils.notebook.run(
    "governance_analysis",
    timeout_seconds=600,
    arguments={"catalog_name": "my_catalog", "schema_name": "my_schema"}
)
```

## Results

- Results saved to `{catalog}.{schema}.governance_results` Delta table
- Summary by category with pass rates and scores
- Overall governance score calculation

## Dashboard

Create visualizations in Databricks SQL Workspace using the queries in the notebook:
- Governance score by category (bar chart)
- Overall governance score (counter)
- Failed checks (table)

## Categories

- Metastore setup (2 checks)
- Identity (7 checks)
- Managed Storage (5 checks)
- Compute/Cluster Policy (1 check)
- Migration Completeness (3 checks)
- Audit & Lineage Coverage (3 checks)
- Privileges (3 checks)
