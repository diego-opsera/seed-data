# Seed Data Generation Plan

## Overview

The goal is to generate realistic, narrative data that tells a sales story about how GitHub Copilot improved developer productivity metrics. We want to do this safely, in phases, without corrupting any existing data.

All data we insert will use a clearly fake, namespaced enterprise name and ID so it can never be mistaken for or collide with real data.

### Source of truth

**`Copilot table definition.md` is the authoritative schema.** The data engineering team provided it directly and it should be treated as correct. If a UAT/dev instance appears to be missing tables or has different types, assume the instance is behind — not that the schema is wrong.

---

## Phase 0 — Exploration (Read-Only)

**Goal:** Understand what's already in the database and what shape the data needs to be before writing a single row.

Everything in this phase is **read-only**. You cannot break anything by running these queries.

### How to run queries in Databricks

1. Log into your Databricks workspace
2. In the left sidebar, click **SQL Editor** (or **Queries**)
3. Make sure the compute dropdown at the top shows an active warehouse/cluster (not "Stopped")
4. Paste a query into the editor and press **Run** (or Cmd+Enter)

> If the warehouse is stopped, click the dropdown and select one — it may take ~30 seconds to start.

---

### Step 0.1 — Check what tables exist and confirm you can see them

```sql
SHOW TABLES IN base_datasets;
```

**What to look for:** A list of table names. Confirm the tables from our schema file are all there. Note any tables that are NOT in our schema file — those might matter later.

---

### Step 0.2 — Check row counts and date ranges for each table

Run these one at a time. They tell us how much real data exists and what time period it covers.

```sql
SELECT COUNT(*) AS row_count, MIN(usage_date) AS earliest, MAX(usage_date) AS latest
FROM base_datasets.enterprise_level_copilot_metrics;
```

```sql
SELECT COUNT(*) AS row_count, MIN(usage_date) AS earliest, MAX(usage_date) AS latest
FROM base_datasets.org_level_copilot_metrics;
```

```sql
SELECT COUNT(*) AS row_count, MIN(copilot_usage_date) AS earliest, MAX(copilot_usage_date) AS latest
FROM base_datasets.github_copilot_metrics_ide_org_level_new;
```

```sql
SELECT COUNT(*) AS row_count, MIN(copilot_usage_date) AS earliest, MAX(copilot_usage_date) AS latest
FROM base_datasets.github_copilot_metrics_ide_teams_level_new;
```

```sql
SELECT COUNT(*) AS row_count, MIN(usage_date) AS earliest, MAX(usage_date) AS latest
FROM base_datasets.enterprise_user_level_copilot_metrics;
```

**What to record:** Row count + date range for each. If a table has 0 rows, note it — it may be unused or newly created.

---

### Step 0.3 — Find all existing enterprise and org names

This is critical. We need to pick a fake enterprise name and ID that doesn't collide with anything real.

```sql
SELECT DISTINCT enterprise_id, enterprise
FROM base_datasets.enterprise_level_copilot_metrics
ORDER BY enterprise_id;
```

```sql
SELECT DISTINCT organization_id, org_name
FROM base_datasets.org_level_copilot_metrics
ORDER BY organization_id;
```

```sql
SELECT DISTINCT org_name
FROM base_datasets.github_copilot_metrics_ide_org_level_new
ORDER BY org_name;
```

**What to record:** All existing enterprise IDs and names, and all org names. We'll pick our fake IDs to be safely outside this range (e.g., if real IDs go up to 500, we'll use 99999).

---

### Step 0.4 — Look at a sample of real rows (including nested columns)

This shows us what real data looks like, especially the complex nested array columns.

```sql
SELECT *
FROM base_datasets.enterprise_level_copilot_metrics
LIMIT 3;
```

```sql
SELECT *
FROM base_datasets.org_level_copilot_metrics
LIMIT 3;
```

```sql
SELECT *
FROM base_datasets.enterprise_user_level_copilot_metrics
LIMIT 3;
```

```sql
SELECT *
FROM base_datasets.github_copilot_metrics_ide_org_level_new
LIMIT 3;
```

**What to look for:**
- What do the `totals_by_feature`, `totals_by_ide`, etc. columns look like when expanded? (Databricks will show them as JSON-like structures)
- Are there any columns that are always NULL? That tells us which ones we can skip.
- What are the actual string values used for `feature`, `ide`, `language`, `model`?

---

### Step 0.5 — Find the enum values used in nested arrays

Instead of guessing what strings like "feature" or "ide" should be, let's pull the real values from the data.

```sql
SELECT DISTINCT feature.feature
FROM base_datasets.enterprise_user_feature_level_copilot_metrics
ORDER BY feature;
```

```sql
SELECT DISTINCT ide
FROM base_datasets.enterprise_user_ide_level_copilot_metrics
ORDER BY ide;
```

```sql
SELECT DISTINCT language, model
FROM base_datasets.enterprise_user_language_model_level_copilot_metrics
ORDER BY language, model;
```

**What to record:** The exact string values used. These are what we'll hardcode into our generator (e.g., `"vscode"` not `"VS Code"`).

---

### Step 0.6 — Confirm whether tables allow deletes

Some Delta tables can be configured as "append-only," meaning you can add rows but never remove them. This matters a lot for our snapshot/cleanup strategy.

```sql
DESCRIBE DETAIL base_datasets.enterprise_level_copilot_metrics;
```

```sql
DESCRIBE DETAIL base_datasets.org_level_copilot_metrics;
```

```sql
DESCRIBE DETAIL base_datasets.github_copilot_metrics_ide_org_level_new;
```

**What to look for:** In the results, find the `properties` column. If you see `"delta.appendOnly": "true"` in that column, the table is append-only and we **cannot delete rows from it**. If that key is absent or set to `"false"`, we can delete.

This changes our cleanup strategy:
- If tables allow deletes → we can insert and later clean up with `DELETE WHERE enterprise = 'our-fake-name'`
- If tables are append-only → we need a snapshot/restore strategy from the start, or a separate schema

---

### Step 0.7 — Check what the frontend actually queries (optional but valuable)

If you have access to Databricks SQL query history, you can see which tables and columns the frontend actually reads. This lets us skip populating tables the frontend doesn't use.

In the Databricks SQL Editor:
1. Click the **History** tab (clock icon in left sidebar, or inside SQL Editor)
2. Look for recent queries — they'll show the SQL and which warehouse ran them
3. Note which `base_datasets.*` tables appear

If you can't access history or it's empty, we'll just populate all the relevant tables to be safe.

---

### Step 0.8 — Record your findings

After running all the queries above, fill in this section and commit it:

```
## Phase 0 Findings

### Table row counts
| Table | Row Count | Date Range |
|---|---|---|
| enterprise_level_copilot_metrics | ? | ? to ? |
| org_level_copilot_metrics | ? | ? to ? |
| enterprise_user_level_copilot_metrics | ? | ? to ? |
| github_copilot_metrics_ide_org_level_new | ? | ? to ? |
| github_copilot_metrics_ide_teams_level_new | ? | ? to ? |
| (add others as needed) | | |

### Existing enterprise IDs and names
(paste results here)

### Existing org names
(paste results here)

### Safe fake enterprise ID to use
enterprise_id: ???
enterprise: "???"

### Feature enum values
(paste results here)

### IDE enum values
(paste results here)

### Language/model pairs
(paste results here)

### Append-only status
| Table | appendOnly? |
|---|---|
| enterprise_level_copilot_metrics | yes/no |
| org_level_copilot_metrics | yes/no |
| github_copilot_metrics_ide_org_level_new | yes/no |

### Frontend query findings
(paste relevant tables/columns if found)
```

---

## Phase 1 — Minimal Smoke Test Dataset

*(To be detailed after Phase 0 findings are in)*

**Goal:** Insert the smallest possible valid dataset for our fake enterprise — just enough to confirm the frontend renders it without errors. No story, no trends. 7 days, 3 users, 1 org, 2 teams.

Key decisions pending Phase 0:
- Confirmed fake `enterprise_id` and `enterprise` name
- Whether we can use `DELETE` for cleanup or need a different strategy
- Exact enum values for features/IDEs/languages

---

## Phase 2 — Narrative Data Generation

*(To be detailed after Phase 1 is validated)*

**Goal:** Generate ~12 months of data that tells this story:

> A company starts using Copilot. In the first month, adoption is low. Over the next few months, acceptance rates climb, chat engagement grows, and agent usage begins. By month 6+, the metrics clearly show improved developer productivity.

**Key metrics that will show the arc:**
- `code_acceptance_activity_count / code_generation_activity_count` — the headline acceptance rate, trending upward
- `daily_active_users` and `monthly_active_users` — growing over time
- `monthly_active_agent_users` — appears partway through (agent adoption)
- `ide_chat_editor_chats` — growing as chat becomes a habit
- `loc_added_sum` — net output trending up

---

## Phase 3 — Snapshot & Story Management

*(To be detailed after Phase 2)*

**Goal:** Be able to switch between stories, reset to clean state, or generate a new use case without manual database work.

**Planned approach:**
- Each story uses a unique `enterprise_id`/`enterprise` namespace — stories can coexist in the DB
- `loader.py export --story <name>` dumps that story's rows to `snapshots/<name>/` as Parquet files
- `loader.py restore --story <name>` re-inserts from those files
- `loader.py delete --story <name>` removes all rows for that enterprise (only works if tables allow deletes — confirmed in Phase 0)
- Snapshots are committed to this repo so they're version-controlled and shareable

---

## Technical Stack (planned)

- **Language:** Python
- **Databricks connection:** `databricks-sdk` (lightweight, no cluster needed for SQL warehouse writes)
- **Local data generation:** Pure Python + standard library (no heavy dependencies)
- **Write method:** Generate Parquet files locally → upload to DBFS → `COPY INTO` table (safe, transactional, reviewable before commit)
- **Validation:** Offline consistency checker runs before any write, requires `--confirm` flag to proceed

---

## Files in this repo (planned)

```
seed-data/
  PLAN.md                      ← this file
  docs/
    exploration.md             ← Phase 0 findings (fill this in)
  config/
    entities.yaml              ← fake enterprise, orgs, teams, users
    stories/
      baseline.yaml            ← story parameters (dates, scale, trend shape)
  generators/
    enterprise_level.py
    org_level.py
    user_level.py
    ide_level.py
    feature_level.py
    language_model_level.py
    usage_report.py
    utils.py                   ← shared struct builders, consistency math
  loader.py                    ← reads generated data, writes to / exports from Databricks
  snapshots/                   ← parquet exports live here
```
