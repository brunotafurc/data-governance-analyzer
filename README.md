# Data Governance Analyzer for Databricks

Evaluates Unity Catalog governance best practices across 25 checks in 6 categories.

## Setup

1. Upload `governance_analyzer.py` to your Databricks workspace
2. Import `governance_analysis.ipynb` as a notebook
3. Update catalog/schema names in the notebook (default: `main.default`)
4. Run the notebook to execute all checks

## Results

- Results saved to `governance_results` Delta table
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
