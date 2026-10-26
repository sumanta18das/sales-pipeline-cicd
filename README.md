# Sales Pipeline: CI/CD on Azure Databricks (Dev to UAT to Prod)

A small medallion pipeline (Bronze, Silver, Gold) that is tested and deployed to
**three separate Azure Databricks workspaces** automatically using **GitHub Actions** and
**Databricks Asset Bundles** (now called *Declarative Automation Bundles*, still "DAB").

```
feature branch --PR--> CI (lint + unit tests + bundle validate)
                         |
                       merge
                         v
main ----> CD:  DEV workspace ---> UAT workspace ---> [approve] ---> PROD workspace
               deploy + run        deploy + run                     deploy + run
               + data checks       + data checks                    + data checks
```

## Project structure

| Path | What it is |
|---|---|
| `databricks.yml` | Bundle config. 3 targets, each pointing to its own workspace |
| `resources/sales_pipeline_job.yml` | The Databricks job: bronze, silver, gold, validate |
| `src/transformations.py` | All business logic as testable functions |
| `src/01_bronze.py` ... `src/99_validate.py` | Databricks notebooks (one per task) |
| `data/orders.csv` | Sample raw data (has duplicates and bad rows on purpose) |
| `tests/` | Unit tests with pytest and local Spark |
| `.github/workflows/ci.yml` | CI: runs on every Pull Request |
| `.github/workflows/cd.yml` | CD: runs on every merge to `main` |
| `.github/workflows/destroy.yml` | Manual cleanup of one environment |
| `scripts/*.sql` | Catalog setup, demo checks, cleanup |
| `docs/classic_cluster_job.yml` | Job version for workspaces without serverless |
| `CICD_DEMO_GUIDE.md` | Full step by step guide |

## Environments

| Target | Workspace | Catalog | GitHub secret | Schedule | Gate |
|---|---|---|---|---|---|
| `dev` | `dbx-cicd-dev` | `dev_catalog` | PAT of DEV workspace | Paused | Automatic |
| `uat` | `dbx-cicd-uat` | `uat_catalog` | PAT of UAT workspace | Paused | Automatic after dev passes |
| `prod` | `dbx-cicd-prod` | `prod_catalog` | PAT of PROD workspace | Daily 6 AM IST | Manual approval |

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt

ruff check .                        # lint
pytest -v                           # unit tests (needs Java 17)

databricks auth login --host <dev-workspace-url>
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev sales_pipeline_job
```

## Golden rules

1. Never edit UAT or Prod jobs in the Databricks UI. Change code, open a PR.
2. The same commit goes to every workspace. Only the host and variables change.
3. No tests passing, no merge. No data checks passing, no promotion.
