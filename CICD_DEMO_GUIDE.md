# CI/CD for Data Platforms: Dev to UAT to Prod in Azure Databricks

**Step by step guide for the instructor (setup + live demo + troubleshooting)**

Setup: **3 separate Azure Databricks workspaces** (DEV, UAT, PROD), Unity Catalog, Databricks Asset Bundles (DAB), GitHub, GitHub Actions, Personal Access Tokens (PAT)

---

## Table of contents

| # | Section | When you need it |
|---|---|---|
| 0 | [What you will build](#0-what-you-will-build) | Read first |
| 1 | [What you need to create (full checklist)](#1-what-you-need-to-create-full-checklist) | Before setup |
| 2 | [Part A: Azure and Databricks setup](#part-a-azure-and-databricks-setup) | 1 to 2 days before |
| 3 | [Part B: Laptop setup](#part-b-laptop-setup) | 1 to 2 days before |
| 4 | [Part C: GitHub setup](#part-c-github-setup) | 1 to 2 days before |
| 5 | [Part D: Full dry run](#part-d-full-dry-run-do-this-before-the-session) | 1 day before |
| 6 | [Part E: Live session script](#part-e-live-session-script) | During the session |
| 7 | [Part F: Troubleshooting](#part-f-troubleshooting) | When something breaks |
| 8 | [Part G: Cleanup](#part-g-cleanup-after-the-session) | After the session |
| 9 | [Part H: Ideas for Part 2](#part-h-ideas-for-part-2-session) | Next session |
| 10 | [Appendix](#appendix) | Reference |

---

## 0. What you will build

### The flow

```
 Laptop                     GitHub                                   Azure Databricks
 ------                     ------                                   ----------------
 feature branch --push-->  Pull Request
                              |
                              v
                          CI workflow
                          1. ruff (lint)
                          2. pytest (local Spark, free)
                          3. bundle validate  ----------------------> DEV workspace (check only)
                              |
                       all green? merge
                              v
                          main branch
                              |
                              v
                          CD workflow
                          Stage 1  deploy + run + checks  ----------> DEV  workspace  (dbx-cicd-dev)
                              |  only if green                         dev_catalog.sales.*
                          Stage 2  deploy + run + checks  ----------> UAT  workspace  (dbx-cicd-uat)
                              |  only if green                         uat_catalog.sales.*
                          [ Manual approval in GitHub ]
                              |
                          Stage 3  deploy + run + checks  ----------> PROD workspace  (dbx-cicd-prod)
                                                                       prod_catalog.sales.*
```

### What CI/CD creates in EACH workspace

| Thing | Where to see it in the workspace | Same in all 3? |
|---|---|---|
| Notebooks + `transformations.py` + `orders.csv` | Workspace > Users > you > `.bundle/sales_pipeline/<env>/files` | Yes, same code |
| Job with 4 tasks | Jobs & Pipelines | Yes, only name, catalog and schedule differ |
| Job tag `git_commit` | Job page > Tags | Yes, same commit SHA |
| Tables `bronze_orders`, `silver_orders`, `gold_daily_sales` | Catalog > `<env>_catalog` > `sales` | Yes, same row counts |
| Job runs | Job page > Runs | One run per CD stage |

### The pipeline (Databricks job)

| Task | Notebook | Reads | Writes | What it does |
|---|---|---|---|---|
| 1. bronze | `src/01_bronze.py` | `data/orders.csv` | `bronze_orders` | Load raw file as text + audit columns |
| 2. silver | `src/02_silver.py` | `bronze_orders` | `silver_orders` | Clean, cast types, drop bad rows, remove duplicates |
| 3. gold | `src/03_gold.py` | `silver_orders` | `gold_daily_sales` | Daily revenue per category |
| 4. validate | `src/99_validate.py` | silver + gold | nothing | Data quality checks. Fails the job if data is bad |

Expected row counts in every workspace: **bronze 31, silver 26, gold 9**.

### Environment design

| Item | DEV | UAT | PROD |
|---|---|---|---|
| Azure Databricks workspace | `dbx-cicd-dev` | `dbx-cicd-uat` | `dbx-cicd-prod` |
| Bundle target | `dev` | `uat` | `prod` |
| Bundle mode | `development` | `production` | `production` |
| Catalog (in that workspace) | `dev_catalog` | `uat_catalog` | `prod_catalog` |
| Job name | `[dev <your-name>] sales_pipeline_dev` | `sales_pipeline_uat` | `sales_pipeline_prod` |
| Schedule | Paused | Paused | Daily 6 AM IST |
| Deployed by | CI/CD (and you, from laptop) | CI/CD only | CI/CD only |
| Gate | Automatic | Automatic (after dev passes) | Manual approval |
| GitHub environment | `dev` | `uat` | `prod` (required reviewer) |
| GitHub secret `DATABRICKS_TOKEN` | PAT from DEV workspace | PAT from UAT workspace | PAT from PROD workspace |

> **Key message for learners:** the code is the same everywhere. Only 3 things change per environment: the **workspace URL**, the **token**, and the **catalog name**. All three are config, not code.

### Why the catalog names are different

All workspaces in the same Azure region (in one Databricks account) share **one Unity Catalog metastore**. Catalog names must be unique in a metastore, so we use `dev_catalog`, `uat_catalog`, `prod_catalog`. A catalog created with **default storage** can be used only from the workspace that created it, so DEV can never read PROD data. You will show this live (Part E5).

---

## 1. What you need to create (full checklist)

### 1.1 Accounts and cloud resources

| # | What | Name used in this guide | Where | Notes |
|---|---|---|---|---|
| 1 | Azure subscription | (yours) | portal.azure.com | |
| 2 | Resource group | `rg-dbx-cicd-demo` | Azure portal | Holds all 3 workspaces. Easy cleanup |
| 3 | Databricks workspace (DEV) | `dbx-cicd-dev` | Azure portal | **Premium** tier (or Trial Premium) |
| 4 | Databricks workspace (UAT) | `dbx-cicd-uat` | Azure portal | Premium, **same region** as DEV |
| 5 | Databricks workspace (PROD) | `dbx-cicd-prod` | Azure portal | Premium, **same region** as DEV |
| 6 | Catalog `dev_catalog` | | **DEV** workspace > Catalog | UI, "Use default storage" |
| 7 | Catalog `uat_catalog` | | **UAT** workspace > Catalog | UI, "Use default storage" |
| 8 | Catalog `prod_catalog` | | **PROD** workspace > Catalog | UI, "Use default storage" |
| 9 | PAT in DEV workspace | `github-cicd-dev` | DEV > Settings > Developer | Copy once |
| 10 | PAT in UAT workspace | `github-cicd-uat` | UAT > Settings > Developer | Copy once |
| 11 | PAT in PROD workspace | `github-cicd-prod` | PROD > Settings > Developer | Copy once |
| 12 | GitHub account | (yours) | github.com | Free plan is fine |
| 13 | GitHub repository (**public**) | `sales-pipeline-cicd` | github.com | Public, so required reviewers work on the free plan |
| 14 | GitHub environment `dev` | | Repo Settings > Environments | Secret = DEV PAT |
| 15 | GitHub environment `uat` | | Repo Settings > Environments | Secret = UAT PAT, branch rule `main` |
| 16 | GitHub environment `prod` | | Repo Settings > Environments | Secret = PROD PAT, branch rule `main`, **required reviewer** |
| 17 | Branch protection on `main` | | Repo Settings > Branches | Require PR + CI checks |

> A PAT works **only in the workspace where you created it**. That is why you need 3 PATs. It is also a nice lesson: the DEV token physically cannot touch PROD.

### 1.2 Values to collect (keep in a notepad during setup)

| Value | Example | Goes to |
|---|---|---|
| DEV workspace URL | `https://adb-1111111111111111.1.azuredatabricks.net` | `databricks.yml` > `targets.dev.workspace.host` |
| UAT workspace URL | `https://adb-2222222222222222.2.azuredatabricks.net` | `databricks.yml` > `targets.uat.workspace.host` |
| PROD workspace URL | `https://adb-3333333333333333.3.azuredatabricks.net` | `databricks.yml` > `targets.prod.workspace.host` |
| DEV PAT | `dapi...` | GitHub environment `dev` > secret `DATABRICKS_TOKEN` |
| UAT PAT | `dapi...` | GitHub environment `uat` > secret `DATABRICKS_TOKEN` |
| PROD PAT | `dapi...` | GitHub environment `prod` > secret `DATABRICKS_TOKEN` |

### 1.3 Tools on your laptop

| Tool | Version | Why |
|---|---|---|
| Git | any recent | Push code |
| VS Code (or any editor) | any | Edit files |
| Python | **3.11** | Run tests locally (PySpark 3.5 does not support 3.13) |
| Java JDK | **17** | Local Spark needs Java |
| Databricks CLI | v1.x (latest) | `databricks bundle ...` commands |
| Browser | any | Azure portal, 3 workspaces, GitHub |

> Tip: use 3 browser profiles or 3 colored tab groups (green DEV, yellow UAT, red PROD). Learners will follow much more easily.

### 1.4 Files in this project

| File | Purpose | Do you need to edit it? |
|---|---|---|
| `databricks.yml` | Bundle + 3 targets (one per workspace) | **Yes**: put your 3 workspace URLs |
| `resources/sales_pipeline_job.yml` | Job definition (serverless) | No |
| `docs/classic_cluster_job.yml` | Job definition for classic clusters | Only if serverless is not available |
| `src/transformations.py` | Business logic | No (you change it live in the demo) |
| `src/01_bronze.py`, `02_silver.py`, `03_gold.py`, `99_validate.py` | Notebooks | No |
| `data/orders.csv` | Sample data (31 rows, some bad on purpose) | No |
| `tests/conftest.py`, `tests/test_transformations.py` | Unit tests | No |
| `pyproject.toml` | pytest + ruff settings | No |
| `requirements-dev.txt` | Local/CI Python packages | No |
| `.github/workflows/ci.yml` | CI on Pull Requests | No |
| `.github/workflows/cd.yml` | CD on merge to main | No |
| `.github/workflows/destroy.yml` | Manual cleanup workflow | No |
| `scripts/01_setup_catalogs.sql` | Catalog steps + grants for a service principal | No (only with a service principal) |
| `scripts/02_check_tables.sql` | Demo queries (run in each workspace) | Change the first line per workspace |
| `scripts/99_cleanup.sql` | Drop catalogs | No |
| `.gitignore` | Keeps local state and secrets out of Git | No |

---

## Part A: Azure and Databricks setup

### A0. If you already did the single workspace setup

All workspaces in the same region share one metastore, so the old catalogs block the new ones ("catalog already exists").

| Option | What to do |
|---|---|
| **Reuse the old workspace as DEV** (fastest) | In the old workspace: `DROP CATALOG IF EXISTS uat_catalog CASCADE;` and `DROP CATALOG IF EXISTS prod_catalog CASCADE;`. Keep `dev_catalog`. Create only 2 new workspaces (UAT, PROD) |
| Start fresh with 3 new workspaces | In the old workspace drop all 3 catalogs, then delete the old workspace |

### A1. Create the 3 workspaces

Do this 3 times (you can start all 3 in parallel, each takes 3 to 5 minutes):

| Step | DEV | UAT | PROD |
|---|---|---|---|
| 1. portal.azure.com > search **Azure Databricks** > **+ Create** | | | |
| 2. Resource group | `rg-dbx-cicd-demo` (create new the first time) | `rg-dbx-cicd-demo` | `rg-dbx-cicd-demo` |
| 3. Workspace name | `dbx-cicd-dev` | `dbx-cicd-uat` | `dbx-cicd-prod` |
| 4. Region | example Central India | **same** as DEV | **same** as DEV |
| 5. Pricing tier | **Premium** | **Premium** | **Premium** |
| 6. Managed resource group name | leave empty | leave empty | leave empty |
| 7. Networking and other tabs | default | default | default |
| 8. **Review + create** > **Create** | | | |
| 9. **Go to resource** > **Launch Workspace** > copy URL up to `.net` | DEV URL | UAT URL | PROD URL |

> Cost: an idle workspace costs very little. You mainly pay when compute runs (each CD run is a few minutes of serverless). Delete the resource group after the session (Part G).

> In real companies each environment is often in a **separate Azure subscription** too. For the demo one resource group is enough.

### A2. Check each workspace

Open **each** workspace and check:

| Check | How | Expected |
|---|---|---|
| Unity Catalog is on | Left menu > **Catalog** | You see `system`, `samples` and a catalog named after the workspace |
| Serverless jobs are on | **Jobs & Pipelines** > **Create** > **Job** > look at **Compute** | **Serverless** is shown. Close without saving |
| Serverless SQL warehouse exists | **SQL Warehouses** | A *Serverless Starter Warehouse* (or create one: type **Serverless**) |

> If **Serverless** is missing in a workspace, see [Part F, row 8](#part-f-troubleshooting).

### A3. Create one PAT in each workspace

Do this in **DEV**, then **UAT**, then **PROD**:

| Step | Action |
|---|---|
| 1 | Profile icon (top right) > **Settings** |
| 2 | Under *User*, click **Developer** |
| 3 | Next to **Access tokens**, click **Manage** > **Generate new token** |
| 4 | Comment: `github-cicd-dev` (or `-uat`, `-prod`). Lifetime: `30` days |
| 5 | Click **Generate**. **Copy the token now** (starts with `dapi`, shown only once). Label it in your notepad: DEV / UAT / PROD |

> Do not mix them up. A DEV token pasted into the `prod` GitHub environment gives *Invalid access token* in the PROD stage.

**What changes because we use a PAT (tell learners this):**

| Topic | With your PAT (this demo) | With a service principal (real companies) |
|---|---|---|
| Who deploys and owns UAT/PROD jobs | You | A robot account per workspace |
| Who runs the jobs | You | The robot account |
| If you leave the company | Token stops, prod breaks | Nothing breaks |
| Access needed on catalogs | You created them, so you own them | Grants needed (`01_setup_catalogs.sql`) |
| GitHub secrets | `DATABRICKS_TOKEN` | `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` |

> Say: "For the demo I use my own tokens. In your company, always use a service principal, because a deployment must not depend on one person."

### A4. Create one catalog in each workspace

Plain `CREATE CATALOG` in SQL fails with *"Metastore storage root URL does not exist"*. Use the UI:

| Step | In DEV workspace | In UAT workspace | In PROD workspace |
|---|---|---|---|
| 1 | Catalog > **+** > **Create catalog** | same | same |
| 2 | Name `dev_catalog`, type **Standard** | Name `uat_catalog` | Name `prod_catalog` |
| 3 | Tick **Use default storage** > **Create** | same | same |

> Create each catalog **in its own workspace**. A catalog created in the DEV workspace cannot be used from PROD.

> You do **not** need to create the `sales` schema. The bronze notebook creates it on the first run.

**Grants:** skip them. You created the catalogs, so your PAT user owns them. Grants are only needed with a service principal (Appendix A6).

---

## Part B: Laptop setup

### B1. Install tools

| Tool | Windows (PowerShell) | macOS (Terminal) | Linux / WSL |
|---|---|---|---|
| Git | `winget install Git.Git` | `brew install git` | `sudo apt install git` |
| Python 3.11 | `winget install Python.Python.3.11` | `brew install python@3.11` | `sudo apt install python3.11 python3.11-venv` |
| Java 17 | `winget install EclipseAdoptium.Temurin.17.JDK` | `brew install openjdk@17` | `sudo apt install openjdk-17-jdk` |
| Databricks CLI | `winget install Databricks.DatabricksCLI` | `brew tap databricks/tap` then `brew install databricks` | `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh \| sh` |

Close and reopen the terminal, then check:

```bash
git --version
python --version          # or: py -3.11 --version  (Windows)
java -version             # should say 17
databricks --version      # should say v1.x or newer
```

### B2. Log in to the DEV workspace only

```bash
databricks auth login --host https://adb-1111111111111111.1.azuredatabricks.net
```

| What happens | What to do |
|---|---|
| It asks for a profile name | Type `dev` and press Enter |
| Browser opens | Log in with your Azure account |
| Terminal says "Profile dev was successfully saved" | Done |

Check it works:

```bash
databricks current-user me --profile dev
```

> **Why only DEV?** Developers deploy to DEV from their laptop. UAT and PROD are deployed **only** by GitHub Actions. Your laptop does not even have login for them. Say this to learners: "If you cannot log in to prod from your laptop, you cannot break prod from your laptop."

---

## Part C: GitHub setup

### C1. Create the repository

| Step | Action |
|---|---|
| 1 | github.com > **+** (top right) > **New repository** |
| 2 | Name: `sales-pipeline-cicd` |
| 3 | Visibility: **Public** (required reviewers on environments only work on public repos for free/Pro/Team plans) |
| 4 | Do **not** add README, .gitignore or license (the project has them) |
| 5 | Click **Create repository** |

### C2. Create the 3 environments, each with its own token

Do this **before** pushing code, because the first push to `main` starts the CD workflow.

| Step | Action |
|---|---|
| 1 | Repo > **Settings** > **Environments** > **New environment** |
| 2 | Name `dev` (exact lowercase) > **Configure environment** |
| 3 | **Environment secrets** > **Add environment secret** |
| 4 | Name `DATABRICKS_TOKEN`, value = **DEV** PAT |
| 5 | Repeat steps 1 to 4 for `uat` (value = **UAT** PAT) and `prod` (value = **PROD** PAT) |

**Extra rules:**

| Environment | Setting | Value | Why |
|---|---|---|---|
| `dev` | Deployment branches and tags | **No restriction** | CI on Pull Requests also uses `dev` to validate the bundle |
| `uat` | Deployment branches and tags | **Selected branches and tags** > Add rule > `main` | Only merged code reaches UAT |
| `prod` | Deployment branches and tags | **Selected branches and tags** > Add rule > `main` | Only merged code reaches PROD |
| `prod` | **Required reviewers** | Tick it, add **yourself** | Manual approval before PROD |
| `prod` | Prevent self-review | **Leave unticked** | So you can approve your own run in the demo |

Click **Save protection rules**.

> Same secret name, different value per environment. The workflow code is identical for all stages. Point this out: "the pipeline does not know which workspace it talks to. The environment decides."

### C3. Put your 3 URLs in the project and push

1. Open `databricks.yml` and replace the 3 `host:` lines:

   ```yaml
   targets:
     dev:
       workspace:
         host: https://adb-1111111111111111.1.azuredatabricks.net   # your DEV URL
     uat:
       workspace:
         host: https://adb-2222222222222222.2.azuredatabricks.net   # your UAT URL
     prod:
       workspace:
         host: https://adb-3333333333333333.3.azuredatabricks.net   # your PROD URL
   ```

2. *(Only if no serverless)* copy `docs/classic_cluster_job.yml` over `resources/sales_pipeline_job.yml`.

3. Push the project:

   ```bash
   cd sales-pipeline-cicd            # the project folder
   git init
   git branch -M main
   git add .
   git commit -m "Initial project: sales pipeline with CI/CD"
   git remote add origin https://github.com/<your-username>/sales-pipeline-cicd.git
   git push -u origin main
   ```

4. Go to the repo > **Actions** tab. You see **CD** running. This is your first end to end test (Part D3).

### C4. Run CI once, then protect `main`

Branch protection can only pick check names that have run at least once.

| Step | Action |
|---|---|
| 1 | Repo > **Actions** > **CI** (left list) > **Run workflow** > branch `main` > **Run workflow** |
| 2 | Wait for both jobs to be green: *Lint and unit tests*, *Validate Databricks bundle* |
| 3 | Repo > **Settings** > **Branches** > **Add classic branch protection rule** |
| 4 | Branch name pattern: `main` |
| 5 | Tick **Require a pull request before merging**. **Untick** *Require approvals* (you are alone, you cannot approve your own PR) |
| 6 | Tick **Require status checks to pass before merging**. Search and add **Lint and unit tests** and **Validate Databricks bundle** |
| 7 | Tick **Do not allow bypassing the above settings** |
| 8 | Click **Create** |

---

## Part D: Full dry run (do this before the session)

### D1. Run tests on your laptop

```bash
cd sales-pipeline-cicd
python -m venv .venv            # Windows: py -3.11 -m venv .venv

# activate
source .venv/bin/activate       # macOS / Linux
.venv\Scripts\Activate.ps1      # Windows PowerShell

pip install -r requirements-dev.txt
ruff check .
pytest -v
```

Expected: `All checks passed!` and `19 passed`.

### D2. Deploy to DEV from your laptop (manual way, to understand bundles)

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev sales_pipeline_job
```

The CLI picks the `dev` login automatically because the host in `databricks.yml` matches.

| Check in the **DEV** workspace | Expected |
|---|---|
| Jobs & Pipelines | `[dev <your-name>] sales_pipeline_dev` with 4 tasks |
| Workspace > Users > you > `.bundle/sales_pipeline/dev/files` | Your code (notebooks + data) |
| Catalog > `dev_catalog` > `sales` | `bronze_orders`, `silver_orders`, `gold_daily_sales` |

> Your laptop deploy and the CI/CD DEV deploy use the same user, so they update the **same** DEV job. That is fine.

### D3. Watch the first CD run (started by your push in C3)

| Stage | Expected | Time |
|---|---|---|
| Deploy to DEV | Green. In the **DEV** workspace: job `[dev <your-name>] sales_pipeline_dev` | 3 to 6 min |
| Deploy to UAT | Green. In the **UAT** workspace: job `sales_pipeline_uat` | 3 to 6 min |
| Deploy to PROD | Yellow **Waiting**. Click **Review deployments** > tick `prod` > **Approve and deploy** | |
| Deploy to PROD | Green. In the **PROD** workspace: job `sales_pipeline_prod`, schedule active | 3 to 6 min |

In each stage, open the step **Show what was deployed**. It prints a link to the job. Clicking it opens the right workspace.

### D4. Check the data in each workspace

In each workspace, SQL Editor > paste `scripts/02_check_tables.sql` > change the first line (`USE CATALOG dev_catalog;` / `uat_catalog` / `prod_catalog`) > Run.

| Workspace | bronze | silver | gold |
|---|---|---|---|
| DEV | 31 | 26 | 9 |
| UAT | 31 | 26 | 9 |
| PROD | 31 | 26 | 9 |

Save the query in each workspace as `CI/CD demo check`, so it is ready during the session.

### D5. Rehearse the demo changes (Part E) once on a test branch

Then close those PRs without merging, so `main` is clean for the live session.

### D6. Pre-session checklist (30 minutes before)

| # | Check |
|---|---|
| 1 | All 3 workspaces open in separate tabs/profiles (DEV, UAT, PROD), you are logged in |
| 2 | In each workspace: Jobs & Pipelines tab + SQL Editor with the saved check query |
| 3 | `databricks current-user me --profile dev` works in terminal |
| 4 | GitHub repo open in a tab: Code, Pull requests, Actions |
| 5 | VS Code open on the project, terminal with `.venv` activated |
| 6 | `git checkout main && git pull` done, no local changes |
| 7 | 3 PATs not expired (each workspace > Settings > Developer > Access tokens) |
| 8 | Font size bigger in VS Code, terminal and browser |

---

## Part E: Live session script

### E0. Session plan (about 2 hours)

| Time | Section | What you show | Key message |
|---|---|---|---|
| 0:00 | Recap | Git flow from last session: branch, commit, push, PR | "Today we automate what happens after the PR" |
| 0:05 | The problem | Story: copy notebook to prod by hand, forgot a change, prod broke | Manual = slow + risky |
| 0:12 | CI vs CD | Table E1 | CI checks code, CD ships code |
| 0:20 | Environments | 3 workspaces side by side (Azure portal + 3 tabs) | Separate places, same code |
| 0:30 | Project tour | Folder structure, `transformations.py`, notebooks | Logic in functions = testable |
| 0:40 | Bundles | `databricks.yml`: 3 targets, 3 hosts, 3 catalogs | Only config changes per environment |
| 0:50 | Manual deploy to DEV | D2 commands live | One command deploys everything |
| 0:55 | Unit tests | `pytest -v` live | Tests run without Databricks, for free |
| 1:00 | CI + CD workflows | Walk through `ci.yml` and `cd.yml`, GitHub environments | Same steps, environment decides the workspace |
| 1:05 | Demo 1 | New feature end to end (E2) | The happy path |
| 1:20 | Workspace tour | What CI/CD created in each workspace (E3) | Same commit everywhere, data isolated |
| 1:30 | Demo 2 | Break the tests (E4) | Bad code cannot be merged |
| 1:40 | Demo 3 | Rollback (E5) | Undo = revert a PR |
| 1:50 | Recap + Q&A | Diagram from section 0, golden rules | |

### E1. Concepts to explain (simple words)

| Term | Simple meaning | In this project |
|---|---|---|
| CI (Continuous Integration) | Every change is checked automatically before it joins main | `ci.yml`: lint, tests, validate |
| CD (Continuous Deployment) | Every merged change is shipped automatically to environments | `cd.yml`: DEV, UAT, PROD workspaces |
| Environment | A separate place with its own data, people and permissions | 3 workspaces |
| Workspace | One Databricks "site" with its own URL, users, jobs and notebooks | `dbx-cicd-dev`, `-uat`, `-prod` |
| Promotion | Moving the **same** version from one environment to the next | DEV -> UAT -> PROD in one CD run |
| Bundle (DAB) | A folder + YAML that describes jobs, notebooks and settings | `databricks.yml` + `resources/` |
| Target | One environment inside the bundle (URL + variables) | `-t dev`, `-t uat`, `-t prod` |
| PAT | A personal token that acts as you, valid in one workspace | `DATABRICKS_TOKEN` in each GitHub environment |
| Service principal | A non-human account used by automation | Not used in this demo. Best practice in companies |
| GitHub environment | A named place in GitHub with its own secrets and rules | `dev`, `uat`, `prod` |
| Quality gate | A check that must pass before moving forward | Unit tests, data checks, approval |

**Talking points:**

- "Bundles are now called *Declarative Automation Bundles* in the docs. Same thing, same commands."
- "Nobody clicks around in prod. If it is not in Git, it does not exist."
- "We test the code in CI and the data in CD."
- "Three workspaces, one codebase."

### E2. Demo 1: Add a new metric end to end (happy path)

**Story:** business wants **average order value** in the gold table.

**Step 1: create a branch**

```bash
git checkout main
git pull
git checkout -b feature/avg-order-value
```

**Step 2: change the code** in `src/transformations.py`, function `build_daily_sales`. Add the `.withColumn(...)` block between `.agg(...)` and `.orderBy(...)`:

```python
def build_daily_sales(silver_df: DataFrame) -> DataFrame:
    """Daily sales per category. Cancelled orders are not counted as revenue."""
    return (
        silver_df.filter(F.col("status") != "CANCELLED")
        .groupBy("order_date", "category")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").cast("bigint").alias("total_quantity"),
            F.sum("amount").cast("decimal(14,2)").alias("total_revenue"),
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_revenue") / F.col("total_orders"), 2),
        )
        .orderBy("order_date", "category")
    )
```

**Step 3: add a test** in `tests/test_transformations.py`, inside `test_build_daily_sales_aggregates_and_skips_cancelled`, after the `total_revenue` line:

```python
    assert elec["avg_order_value"] == Decimal("125.00")
```

**Step 4: test locally**

```bash
pytest -v
```

**Step 5: push and open a PR**

```bash
git add .
git commit -m "Add avg_order_value to gold daily sales"
git push -u origin feature/avg-order-value
```

GitHub shows **Compare & pull request** > **Create pull request**.

**Step 6: watch CI** on the PR page: both checks turn green, **Merge** button becomes active. Point out: before merge, UAT and PROD workspaces are untouched.

**Step 7: merge**: click **Merge pull request** > **Confirm**. Note the short commit SHA shown on the merged PR (example `a1b2c3d`).

**Step 8: watch CD**: Actions tab > CD run.

| What to point at | Say |
|---|---|
| DEV green | "Deployed and ran in the DEV workspace. Data checks passed." |
| UAT green | "Same commit, now in a completely different workspace." |
| PROD waiting | "Nobody can ship to prod without approval." |
| Click Review deployments > Approve and deploy | "In a company this is the lead or release manager." |
| PROD green | "Done. Nobody logged in to the PROD workspace." |

**Step 9: show the result in the PROD workspace**, SQL Editor:

```sql
USE CATALOG prod_catalog;
SELECT * FROM sales.gold_daily_sales ORDER BY order_date, category;
DESCRIBE HISTORY sales.gold_daily_sales;
```

Point at the new `avg_order_value` column, and in history at the time it was written: a minute ago, by the CD run.

### E3. Workspace tour: what CI/CD created in each workspace

Do this right after Demo 1. Open the 3 workspace tabs side by side and go through this table **in each workspace**:

| # | Where in the workspace | What to point at | Say |
|---|---|---|---|
| 1 | **Jobs & Pipelines** | `sales_pipeline_dev` / `_uat` / `_prod` | "Nobody created these jobs by hand. The bundle did." |
| 2 | Open the job > **Tasks** | bronze > silver > gold > validate | "Same 4 tasks in every workspace." |
| 3 | Job > **Tags** | `git_commit` = the SHA from Step 7 (same in all 3) | "This proves the same code version runs everywhere." |
| 4 | Job > **Parameters** | `catalog` = `dev_catalog` / `uat_catalog` / `prod_catalog` | "Only the config is different." |
| 5 | Job > **Schedule** | Paused in DEV and UAT, active in PROD | "Prod runs every morning. Dev and UAT only when we deploy." |
| 6 | Job > **Runs** | Latest run started a few minutes ago by CD | "GitHub ran this job, not a person." |
| 7 | **Workspace** > Users > you > `.bundle/sales_pipeline/<env>/files/src` | Open `transformations.py`, scroll to `avg_order_value` | "The code arrived here from Git. Same file in all 3." |
| 8 | **Catalog** > `<env>_catalog` > `sales` | 3 tables, `gold_daily_sales` has the new column | "Each workspace has its own data." |
| 9 | Job page top (if shown) | Note that the job was deployed from a bundle | "Edit it in Git, not here." |

**Isolation demo** (in the **DEV** workspace SQL Editor):

```sql
SELECT * FROM prod_catalog.sales.gold_daily_sales;
```

Expected: an error. Say: "From DEV you cannot touch PROD data. Each catalog belongs to its own workspace."

**Token demo** (optional, 1 minute): show GitHub > Settings > Environments: 3 environments, each with its own `DATABRICKS_TOKEN`. Say: "The UAT token was created in the UAT workspace. It does not work anywhere else."

### E4. Demo 2: Break the tests (CI blocks bad code)

**Story:** a developer "simplifies" a rule and lets zero quantity orders in.

```bash
git checkout main
git pull
git checkout -b feature/allow-zero-qty
```

In `src/transformations.py`, function `clean_orders`, change:

```python
        & (F.col("quantity") > 0)
```

to:

```python
        & (F.col("quantity") >= 0)
```

Push **without** running tests locally (on purpose):

```bash
git add .
git commit -m "Allow zero quantity orders"
git push -u origin feature/allow-zero-qty
```

Open the PR and show:

| What you see | Say |
|---|---|
| *Lint and unit tests* red | "CI caught it in 2 minutes." |
| Click **Details** > failing test `test_clean_orders_drops_bad_rows[bad_row4-zero quantity]` | "The test tells us exactly which rule broke." |
| *Validate Databricks bundle* skipped | "No point checking Databricks if the code is broken." |
| **Merge** button blocked | "Branch protection: red = cannot merge. Not even the admin." |
| 3 workspaces: job tags still show the old commit | "Bad code never reached any workspace." |

Fix it (change back to `> 0`), commit, push to the same branch: checks go green. Then **close the PR without merging** (the change was not wanted).

### E5. Demo 3: Rollback

**Story:** business says "remove the avg_order_value column, it confuses the dashboard".

| Step | Action |
|---|---|
| 1 | Open the merged PR from Demo 1 > click **Revert** (bottom of the PR) |
| 2 | GitHub creates a new branch + PR that undoes the change > **Create pull request** |
| 3 | CI runs > green > **Merge** |
| 4 | CD runs > DEV > UAT > approve PROD |
| 5 | PROD workspace: `SELECT * FROM prod_catalog.sales.gold_daily_sales` > column is gone |
| 6 | PROD job > Tags > `git_commit` changed to the revert commit |

Say: "Rollback is just another change going through the same pipeline. It is tested and approved like any other change."

### E6. Optional extra demo: someone edits prod by hand

| Step | Action |
|---|---|
| 1 | In the **PROD** workspace, open `sales_pipeline_prod` > edit the description or the schedule time > Save |
| 2 | Run CD again: Actions > CD > **Run workflow** (main), approve prod |
| 3 | Open the job again: your manual change is gone |

Say: "Git is the source of truth. Manual changes are overwritten on the next deploy."

### E7. Closing slide: golden rules

| # | Rule |
|---|---|
| 1 | Everything is in Git: code, job config, environment settings |
| 2 | Same code in every workspace, only URL, token and variables change |
| 3 | Keep logic in functions so you can unit test it |
| 4 | CI on every PR, CD on every merge |
| 5 | Only the pipeline deploys to UAT and PROD. In companies it uses a service principal |
| 6 | Prod needs approval |
| 7 | Never edit UAT/Prod in the UI |

---

## Part F: Troubleshooting

| # | Problem / error | Cause | Fix |
|---|---|---|---|
| 1 | `CREATE CATALOG` fails: *Metastore storage root URL does not exist* | No metastore root storage | Create the catalog in the **UI** with **Use default storage** (Part A4) |
| 2 | Creating `uat_catalog` / `prod_catalog` fails: *already exists* | Old catalogs from the single workspace setup (shared metastore) | Drop them in the old workspace (Part A0) |
| 3 | Job fails: `CATALOG_NOT_FOUND` or no access to `uat_catalog` in UAT | Catalog was created in the wrong workspace | Drop it there, create it again **in the UAT workspace** |
| 4 | CI/CD: `default auth: cannot configure default credentials` | Secret missing in that GitHub environment, or wrong name | Check the environment has a secret named exactly `DATABRICKS_TOKEN` |
| 5 | Only one stage fails with `401` / `403` / `Invalid access token` | Wrong PAT in that environment (for example the DEV PAT in `prod`), or it expired | Create a new PAT **in that stage's workspace**, update only that environment's secret |
| 6 | Laptop: `bundle validate -t uat` asks for login / fails | Laptop is only logged in to DEV | Expected. UAT and PROD are deployed by GitHub only |
| 7 | Default storage catalog fails from a classic cluster | Default storage works only with serverless | Use serverless job compute and a serverless SQL warehouse |
| 8 | Job fails: serverless not enabled / compute not available | That workspace has no serverless jobs | Use `docs/classic_cluster_job.yml` and a catalog with a managed location (needs ADLS + external location). Easier: try a workspace in another region where serverless is available |
| 9 | Job fails: `ModuleNotFoundError: No module named 'transformations'` | Notebook cannot see the file next to it | Keep `transformations.py` in the same `src` folder. If still failing, add as first code cell: `import os, sys; sys.path.append(os.path.abspath("."))` |
| 10 | Bronze fails: `FileNotFoundError ... orders.csv` | Data file not deployed | Check `data/orders.csv` is committed (`git ls-files data`) |
| 11 | Local `pytest`: `Java gateway process exited` / `JAVA_HOME is not set` | Java missing | Install Java 17, set `JAVA_HOME`, reopen terminal |
| 12 | Local `pip install pyspark` fails or `PYTHON_VERSION_MISMATCH` | Python 3.12+/3.13 or many Pythons | Use Python **3.11** venv. `tests/conftest.py` already sets `PYSPARK_PYTHON` |
| 13 | Windows: Spark warnings about `winutils` / `HADOOP_HOME` | Normal on Windows | Tests still pass. If they do not, use WSL or rely on CI |
| 14 | `prod` environment has no *Required reviewers* option | Private repo on a free/Pro/Team plan | Make the repo **public** (Settings > General > Danger zone > Change visibility) |
| 15 | PR says *Review required*, cannot merge | Branch rule requires approvals | Untick **Require approvals** in the branch rule |
| 16 | Status checks not listed in branch protection | CI never ran | Run CI once (Part C4 step 1) |
| 17 | *Branch "..." is not allowed to deploy to uat* | Environment branch rule | Expected for feature branches. Only `main` can deploy to UAT/PROD |
| 18 | `deploy lock acquired by ...` | A previous run was cancelled mid deploy | Re-run the workflow. If stuck, run once with `databricks bundle deploy -t <target> --force-lock` |
| 19 | Deploy says it needs approval to delete or recreate resources | Bundle wants to delete something | Check the change. If OK, add `--auto-approve` to that deploy step for one run |
| 20 | `git_commit` tag is empty | Bundle was deployed from a folder that is not a Git repo | Deploy from the cloned repo (CI/CD always is) |
| 21 | Prod job keeps running daily after the session | Schedule is UNPAUSED in prod | Run the Destroy workflow (Part G) |

**Where to look for errors:**

| Problem shows in | Open |
|---|---|
| GitHub Actions step red | Click the step, read the last 20 lines |
| `bundle run` failed | The log prints a **Run URL**. Open it (it opens the right workspace) > click the red task > see the error |

---

## Part G: Cleanup after the session

| Step | Action |
|---|---|
| 1 | GitHub > Actions > **Destroy environment** > Run workflow > target `prod` > approve |
| 2 | Repeat for `uat` and `dev` |
| 3 | In each workspace SQL Editor: run the matching line of `scripts/99_cleanup.sql` |
| 4 | In each workspace: Settings > Developer > Access tokens > **revoke** the `github-cicd-*` token |
| 5 | GitHub: delete the 3 environments (or their secrets) |
| 6 | Azure portal: delete resource group `rg-dbx-cicd-demo` (removes all 3 workspaces) |

---

## Part H: Ideas for Part 2 session

| Topic | Why it matters |
|---|---|
| Move from PAT to a service principal (one per workspace) | Deploys do not depend on a person |
| OIDC / workload identity federation | No secrets stored in GitHub at all (Appendix A3) |
| Separate Azure subscriptions per environment | Billing and security isolation like in big companies |
| Pin the CLI version (`databricks/setup-cli@vX.Y.Z`) | Same CLI every run |
| Build a Python wheel instead of loose files | Proper packaging and versioning |
| Release tags (`v1.2.0`) | Deploy prod only from a tag |
| Lakeflow Declarative Pipelines in bundles | Streaming / DLT style pipelines |
| Incremental loads (Auto Loader, MERGE) | Real ingestion instead of overwrite |
| Terraform for infra (workspaces, catalogs, SPs) | Create the 3 workspaces from code too |
| Notifications (email / Teams) on failure | Ops readiness |

---

## Appendix

### A1. Going back to one workspace (if needed)

Put the **same URL** in all 3 `host:` lines in `databricks.yml`, and create all 3 catalogs in that one workspace. In GitHub, put the same PAT in all 3 environments. Nothing else changes.

### A2. Command cheat sheet

| Command | What it does |
|---|---|
| `databricks auth login --host <url>` | Log in from laptop (your user) |
| `databricks current-user me --profile dev` | Who am I in the DEV workspace |
| `databricks bundle validate -t dev` | Check config is correct |
| `databricks bundle deploy -t dev` | Upload code + create/update the job |
| `databricks bundle run -t dev sales_pipeline_job` | Run the job and wait for the result |
| `databricks bundle summary -t dev` | Show what is deployed, with links |
| `databricks bundle open -t dev sales_pipeline_job` | Open the job in the browser |
| `databricks bundle destroy -t dev` | Delete the job and files of that target |
| `ruff check .` | Lint |
| `pytest -v` | Unit tests |

### A3. Upgrade: OIDC (no secret in GitHub)

With workload identity federation, GitHub gets a short-lived token from Databricks for each run. No token or secret is stored in GitHub. This needs a service principal per workspace.

| Step | Action |
|---|---|
| 1 | Create a **federation policy** for GitHub Actions on each service principal (Databricks account console). Subject example: `repo:<owner>/sales-pipeline-cicd:environment:prod` |
| 2 | In each workflow job add the permission and env vars below |
| 3 | Delete `DATABRICKS_TOKEN` from GitHub environments |

```yaml
permissions:
  id-token: write
  contents: read

jobs:
  deploy-prod:
    environment: prod
    env:
      DATABRICKS_AUTH_TYPE: github-oidc
      DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}   # PROD service principal ID
```

Docs: "Enable workload identity federation for GitHub Actions" on learn.microsoft.com (Azure Databricks).

### A4. How the environment value flows (explain this slowly)

```
GitHub environment "prod"
   secret DATABRICKS_TOKEN = PAT from PROD workspace
        |
        v
cd.yml  deploy-prod:  databricks bundle deploy -t prod
        |
        v
databricks.yml  targets.prod
   workspace.host     = https://adb-3333...azuredatabricks.net   (WHERE)
   variables.catalog  = prod_catalog                             (WHICH DATA)
        |
        v
resources/sales_pipeline_job.yml   parameters: catalog = ${var.catalog}
        |
        v
PROD workspace: job sales_pipeline_prod, parameter catalog = prod_catalog
        |
        v
Notebook: dbutils.widgets.get("catalog")  ->  "prod_catalog"
        |
        v
saveAsTable("prod_catalog.sales.gold_daily_sales")
```

### A5. What the sample data contains (so you can explain silver)

| Row | Problem | What silver does |
|---|---|---|
| ORD-1005 (2 times) | Duplicate | Keeps one |
| ORD-1012 | Blank customer_id | Dropped |
| ORD-1018 | Quantity -2 | Dropped |
| ORD-1021 | Price "abc" | Dropped (safe cast gives NULL) |
| ORD-1024 | Date 2026-13-45 | Dropped (safe cast gives NULL) |
| ORD-1007 | Status `completed` (lowercase) | Becomes `COMPLETED` |
| ORD-1027 | Status ` cancelled ` (spaces) | Becomes `CANCELLED`, excluded from revenue |
| ORD-1029 | Category ` electronics` | Becomes `Electronics` |

31 raw rows -> 26 clean rows -> 9 gold rows (3 days x 3 categories).

### A6. Switch from PAT to service principals (when you get access)

Best practice: **one service principal per workspace** (`sp-cicd-dev`, `sp-cicd-uat`, `sp-cicd-prod`).

| Step | Action |
|---|---|
| 1 | Admin creates an SP in each workspace: Settings > Identity and access > Service principals > Add > **Databricks managed**, tick **Workspace access** |
| 2 | Admin generates a secret for each: open the SP > **Secrets** > **Generate secret**. Gives Client ID (Application ID) and Secret |
| 3 | In each workspace, run the matching GRANT from `scripts/01_setup_catalogs.sql` with that workspace's SP ID |
| 4 | In each GitHub environment: add `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` of that workspace's SP, delete `DATABRICKS_TOKEN` |
| 5 | In `ci.yml`, `cd.yml`, `destroy.yml`: replace the `DATABRICKS_TOKEN` line with the two lines below |
| 6 | Before merging: run **Destroy environment** for uat and prod with the old PATs, so you do not get two copies of the jobs |
| 7 | Merge through a PR. Jobs are recreated under each SP |

```yaml
    env:
      DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
      DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
```
