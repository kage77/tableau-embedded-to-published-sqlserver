# Tableau Embedded to Published SQL Server Migrator

Open-source Python utility designed to migrate Tableau workbooks from **embedded SQL Server connections** to **governed published data sources**.

---

## Why this was created

Tableau provides strong native migration capabilities, including the **Content Migration Tool**, which helps move content between environments (Server, Cloud, sites, projects, etc.).

However, native migration tooling is primarily designed to:

- move content between environments
- preserve existing workbook structures
- migrate content largely *as-is*

In many real-world environments, the challenge is different.

Teams often need to:

- standardize embedded connections
- reduce duplicated extracts
- centralize data access
- enforce governance through published data sources

This project was created to address that specific gap.

### What this script focuses on

Instead of just moving content, this utility performs a **governance refactor**:

- detects embedded SQL Server connections
- creates or reuses governed published data sources
- rewires workbooks to the centralized datasource layer

The goal is not only migration, but long-term maintainability.

---

## Why I designed this

In many Tableau environments, embedded connections grow organically and create challenges:

- duplicated extracts
- inconsistent KPI definitions
- difficult permission management
- higher long-term maintenance overhead

I designed this script as a reusable migration framework to standardize data access through published data sources while keeping the workflow transparent and configurable.

### Design goals

- **Environment agnostic**: no hardcoded credentials or company-specific settings
- **Public-safe**: secrets are injected via environment variables
- **Modular design**: API logic, SQL parsing, and orchestration are separated
- **Dry-run first** workflow to reduce migration risk

---

## What happens inside the script

At a high level, the migration process works like this:

1. **Input parsing**
   - Reads workbook list from CSV.
   - Loads configuration and credential mappings.

2. **Authentication**
   - Signs into Tableau using PAT authentication.
   - Establishes a REST API session.

3. **Workbook analysis**
   - Retrieves workbook connection metadata.
   - Detects embedded SQL Server connections.
   - Extracts server and database information.

4. **Datasource decision**
   - Builds standardized datasource naming.
   - Checks whether a published datasource already exists.
   - Creates or reuses it.

5. **Workbook rewiring**
   - Updates workbook connections to point to the published datasource.

6. **Result reporting**
   - Writes status output (updated / skipped / error).
   - Supports dry-run mode to preview actions.

---

## High-level workflow

1. Read a list of target workbooks  
2. Inspect workbook connections via Tableau REST API  
3. Detect embedded SQL Server connections  
4. Publish or reuse a governed data source  
5. Rewire the workbook to the published data source  
6. Write migration results for review  

---

## Repository structure

```
migrate_v2.py                 # Main migration entry script
config.py                     # Environment-based configuration (no secrets)
tableau_migrator/             # Core migration engine
files/
  credentials.template.csv    # SQL credentials template (copy locally)
  sample-input.template.csv   # Workbook input template
  temp/                       # Runtime scratch (gitignored)
  output/                     # Results output (gitignored)
```

---

## Setup

### 1) Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure Tableau authentication

```bash
cp .env.example .env
```

Fill in:

- TABLEAU_SERVER_URL
- TABLEAU_SITE_NAME
- TABLEAU_PAT_NAME
- TABLEAU_PAT_SECRET

### 3) Configure SQL Server credentials (local only)

```bash
cp files/credentials.template.csv files/credentials.csv
```

Fill in your server/database credentials.  
This file is gitignored and must never be committed.

### 4) Provide workbook input

```bash
cp files/sample-input.template.csv files/sample-input.csv
```

Populate workbook IDs (recommended).

---

## Run

Start with a dry run:

```bash
python migrate_v2.py --input files/sample-input.csv --dry-run
```

Execute migration:

```bash
python migrate_v2.py --input files/sample-input.csv
```

---

## Security model

- No credentials are stored in code
- `.env` and `files/credentials.csv` are excluded via `.gitignore`
- Templates are provided to show required structure without exposing secrets

---

## License

MIT
