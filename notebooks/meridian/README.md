# Meridian Analytics — Data Team Demo Story

## Customer Narrative

**Meridian Analytics** is a mid-size analytics company with a 10-person data engineering team.
Their product is a suite of data pipelines that transform raw operational data into
analytics-ready datasets for their clients. All pipeline work happens in Databricks.

### The Problem (Before Opsera — Apr through Sep 2025)

Promoting validated data pipelines from the Databricks **test workspace** to **production**
is entirely manual:

1. Engineer opens a Jira ticket to request a promotion
2. Data lead triages and estimates scope (2+ weeks)
3. Team schedules a maintenance window (4–6 week wait)
4. Manual execution: copy notebooks, update configs, re-register datasets in the Unity Catalog
5. Manual data quality validation in prod (1–2 weeks)
6. Rollback = re-running old notebooks by hand

**Result:** Each change takes **3–5 months** from ticket to prod. Teams batch everything
into quarterly "big bang" releases. High failure rate (~35%) because of multi-step manual
coordination. When something breaks, restoring data integrity takes 10–20 days.

Engineers spend 40%+ of their time on promotion overhead instead of building.

### Opsera Onboarding (Oct–Nov 2025)

Meridian connects their Databricks workspaces to Opsera pipelines:

- PR-based workflow: engineer raises a PR → Opsera triggers automated tests in staging →
  auto-promotes to prod on approval
- Automated data quality gates built into the pipeline
- Async approval workflow (no maintenance windows, no scheduling meetings)
- Instant rollback: Opsera snapshots workspace state before every promotion

### After Opsera (Dec 2025–Apr 2026)

| DORA Metric | Before | After | DORA Band |
|---|---|---|---|
| Deployment Frequency | 2–3 / month | 5–10 / week | Low → Elite |
| Lead Time for Changes | 90–150 days | 5–7 days | Low → High |
| Change Failure Rate | 35% | 8% | Low → High |
| MTTR | 10–20 days | 4–6 hours | Low → Elite |

Engineers now spend 80% of their time building. Stakeholders get data product changes
in days instead of quarters.

---

## Hierarchy Mapping

This is how Meridian appears in the DORA board hierarchy filter (mirrors the screenshot
showing Organization / Product / Application / Project):

```
Organization:  (empty — enterprise level)
Product:       demo-meridian          ← level_3 in filter_groups_unity
Application:   (empty)
Project:       Meridian Analytics     ← level_1 in filter_groups_unity
```

In filter_groups_unity:
```
level_1 = 'Meridian Analytics'
level_2 = ''
level_3 = 'demo-meridian'
level_4 = ''
level_5 = ''
createdBy = 'seed-data-meridian@demo.io'   ← distinct from Acme's 'seed-data@demo.io'
```

---

## DORA Data Story Arc

The inflection point is at **t = 0.5** (halfway through the date range, ~Oct 2025).
Pre-inflection = manual era. Post-inflection = Opsera era.

### Deployment Frequency (pipeline_activities)

```
Pre  (t=0.0→0.5):  lerp(2, 4, t_phase)   deploys/month   → 2–4/month
Post (t=0.5→1.0):  lerp(8, 60, t_phase)  deploys/month   → 8–60/month (ramping to near-daily)
```

Failure rate:
```
Pre:  lerp(0.35, 0.30, t_phase)
Post: lerp(0.30, 0.08, t_phase)
```

### Lead Time for Changes (pipeline_deployment_commits)

LTFC = time from commit timestamp to deploy timestamp (commit is backdated).

```
Pre  (t < 0.5):  ctfc_days = lerp(150, 90, t_phase)   # 3–5 months
Post (t >= 0.5): ctfc_days = lerp(90,  5,  t_phase)   # dropping to 5 days
```

The commit is placed at `deploy_time - ctfc_days * 24h * jitter_factor`.
This is exactly how dora_charts.py works — same pattern.

### Change Failure Rate + MTTR (cfr_mttr_metric_data)

CFR failure rate per deploy:
```
Pre:  lerp(0.35, 0.30, t_phase)
Post: lerp(0.30, 0.08, t_phase)
```

MTTR (average hours to resolve an incident):
```
Pre:  lerp(480, 240, t_phase)   # 10–20 days
Post: lerp(240, 5,   t_phase)   # dropping to 4–6 hours
```

Issue project name: `'Meridian Data Platform'` (used by CFR/MTTR filter_values_unity entry)
Issue keys: `MDP-{seq:04d}` for change issues, `MDP-INC-{seq:04d}` for incident issues

### Cycle Time for Changes (CTFC — mt_itsm_issues_hist)

Data promotion Jira tickets (project `MDP`):
```
Pre:  cycle time lerp(120, 60, t_phase) days   # 2–4 months
Post: cycle time lerp(60,  5,  t_phase) days   # dropping to 1 week
```

CTFC filter values:
- project_name: `['MDP']`
- issue_status: `['Done', 'done', 'Completed']`
- include_issue_types: `['Story', 'story', 'Bug', 'bug', 'Task', 'task']`
- board_ids: `['2']`           ← board_id=2 (Acme uses 1; must not collide)
- defect_type: `['Bug', 'bug']`

Need to insert `board_id=2` into `source_to_stage.raw_jira_boards_ci`.

---

## Implementation Plan

### New Files to Create

```
generators/dora_meridian.py          ← all Meridian DORA data generation
notebooks/meridian/insert.py         ← runs generator + inserts filter rows
notebooks/meridian/delete.py         ← scoped deletion
```

### Modify

```
notebooks/insert.py                  ← add meridian step (after dora/)
notebooks/delete.py                  ← add meridian step (before dora/)
```

---

### Step 1 — `generators/dora_meridian.py`

Model after `generators/dora_charts.py`. Key differences:

```python
_PROJECT_URL   = "https://github.com/demo-meridian/data-platform.git"
_ISSUE_PROJECT = "Meridian Data Platform"
_RECORD_BY     = "seed-data-meridian"        # MUST differ from Acme's 'seed-data'
_PIPELINE_ID   = "b2c3d4e5-0000-0000-0000-meridian0001"
_REPO_ID       = "demo-meridian-001"
_OWNER         = "demo-meridian"
_JIRA_PROJECT  = "MDP"
_BOARD_ID      = 2
```

Use `story["start_date"]` and `story["end_date"]` to derive `_MONTHS` dynamically
(unlike dora_charts.py which hardcodes them) so the rolling 1-year window always works:

```python
from datetime import date
from calendar import monthrange

def _build_months(start: date, end: date):
    months = []
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        months.append((y, m))
        m += 1
        if m > 13: m = 1; y += 1
    return months
```

Inflection index = `len(months) // 2` (month 6 of 12).

Piecewise t within each phase:
```python
def _phase_t(i, inflection_idx, total):
    if i < inflection_idx:
        return i / max(inflection_idx - 1, 1)        # 0→1 in pre phase
    else:
        return (i - inflection_idx) / max(total - inflection_idx - 1, 1)  # 0→1 in post phase
```

The generator produces three tables (same INSERT headers as dora_charts.py):
- `pipeline_activities` — DF chart data
- `pipeline_deployment_commits` — LTFC chart data
- `cfr_mttr_metric_data` — CFR + MTTR chart data

It does NOT produce CTFC itsm issues (those come from a separate section in insert.py
that calls raw SQL directly, similar to how dora/insert.py handles raw_jira_boards_ci).

Actually: write the CTFC itsm issue INSERTs in the generator too, targeting
`transform_stage.mt_itsm_issues_hist` and `mt_itsm_issues_current`. Study
`generators/itsm_issues.py` for the exact column list and board_info STRUCT format.
Key columns: `customer_id = 'demo-meridian'`, `project_name = 'MDP'`, `board_info`
containing `board_id = 2` (as bigint).

### Step 2 — `notebooks/meridian/insert.py`

```python
import sys, os, uuid, yaml
# module cache-busting (same pattern as direct/insert.py)
for _key in list(sys.modules.keys()):
    if _key.startswith("generators"): del sys.modules[_key]
sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

from generators import dora_meridian

CATALOG = "playground_prod"
story   = yaml.safe_load(open("config/stories/narrative.yaml"))
entities = yaml.safe_load(open("config/entities.yaml"))

# Part 1: DORA base-table data
for sql in dora_meridian.generate(CATALOG, entities, story):
    spark.sql(sql)

# Part 2: filter_groups_unity
FGU_ID          = str(uuid.uuid4())
FILTER_GROUP_ID = str(uuid.uuid4())

# Use distinct createdBy so delete.py can scope independently of Acme rows
CREATED_BY = 'seed-data-meridian@demo.io'

# KPI UUIDs — same as dora/insert.py (shared KPI config, different filter group)
DF_KPI   = "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9"
LTFC_KPI = "a9337c02-a00e-40ad-9cdc-2d18dfd771c9"
CFR_KPI  = "ab9a59ba-a19c-4358-b195-1648797f77c2"
MTTR_KPI = "906f4f2b-a299-4b24-9a24-2330f45dd493"
CTFC_KPIS = [...]   # same 6 UUIDs as dora/insert.py

spark.sql(f"""
    INSERT INTO {CATALOG}.master_data.filter_groups_unity (...)
    VALUES ('{FGU_ID}', 'Meridian Analytics', '', 'demo-meridian', '', '',
            '{FILTER_GROUP_ID}', '{CREATED_BY}', CURRENT_TIMESTAMP(), ...)
""")

# DF + LTFC filters
_fvu(FILTER_GROUP_ID, 'github', 'project_url',
     ['https://github.com/demo-meridian/data-platform.git'],
     f"'{DF_KPI}', '{LTFC_KPI}'", 0, CREATED_BY)
# ... deployment_stages, pipeline_status_success

# CFR + MTTR
_fvu(FILTER_GROUP_ID, 'jira', 'project_name',
     ['Meridian Data Platform'],
     f"'{CFR_KPI}', '{MTTR_KPI}'", 3, CREATED_BY)

# CTFC (board_id=2)
_fvu(FILTER_GROUP_ID, 'jira', 'project_name',   ['MDP'], CTFC_KPIS_SQL, 4, CREATED_BY)
_fvu(FILTER_GROUP_ID, 'jira', 'issue_status',   ['Done', 'done', 'Completed'], CTFC_KPIS_SQL, 5, CREATED_BY)
_fvu(FILTER_GROUP_ID, 'jira', 'include_issue_types', ['Story','story','Bug','bug','Task','task'], CTFC_KPIS_SQL, 6, CREATED_BY)
_fvu(FILTER_GROUP_ID, 'jira', 'board_ids',      ['2'], CTFC_KPIS_SQL, 7, CREATED_BY)
_fvu(FILTER_GROUP_ID, 'jira', 'defect_type',    ['Bug', 'bug'], CTFC_KPIS_SQL, 8, CREATED_BY)

# Jira board (board_id=2 — must not collide with Acme's board_id=1)
spark.sql(f"""
    INSERT INTO {CATALOG}.source_to_stage.raw_jira_boards_ci (message, board_id, board_name, board_type, org_name)
    VALUES ('{{"id": 2, "name": "MDP Board", "type": "scrum"}}', 2, 'MDP Board', 'scrum', 'demo-meridian')
""")
```

### Step 3 — `notebooks/meridian/delete.py`

Scope all deletions by the distinct `_RECORD_BY` and `createdBy` values:

```python
spark.sql(f"DELETE FROM {CATALOG}.base_datasets.pipeline_activities WHERE record_inserted_by = 'seed-data-meridian'")
spark.sql(f"DELETE FROM {CATALOG}.base_datasets.pipeline_deployment_commits WHERE record_inserted_by = 'seed-data-meridian'")
spark.sql(f"DELETE FROM {CATALOG}.base_datasets.cfr_mttr_metric_data WHERE record_inserted_by = 'seed-data-meridian'")
spark.sql(f"DELETE FROM {CATALOG}.transform_stage.mt_itsm_issues_hist WHERE customer_id = 'demo-meridian'")
spark.sql(f"DELETE FROM {CATALOG}.transform_stage.mt_itsm_issues_current WHERE customer_id = 'demo-meridian'")
spark.sql(f"DELETE FROM {CATALOG}.master_data.filter_values_unity WHERE created_by = 'seed-data-meridian@demo.io'")
spark.sql(f"DELETE FROM {CATALOG}.master_data.filter_groups_unity WHERE createdBy = 'seed-data-meridian@demo.io'")
spark.sql(f"DELETE FROM {CATALOG}.source_to_stage.raw_jira_boards_ci WHERE org_name = 'demo-meridian'")
```

### Step 4 — Update Master Scripts

In `notebooks/insert.py`, add after the dora/ step:
```python
exec(open(f"{BASE}/meridian/insert.py").read(), globals())
```

In `notebooks/delete.py`, add before the dora/ step:
```python
exec(open(f"{BASE}/meridian/delete.py").read(), globals())
```

---

## Direct Metrics (Phase 2 — optional follow-up)

The data team uses Copilot to write PySpark jobs and SQL transformations. To show this
in the "Direct" / DevEx dashboard:

1. Add `demo-meridian` as a third org in `config/entities.yaml`
2. Create `config/stories/meridian_narrative.yaml` with:
   - user_count_start: 3, user_count_end: 12 (small team growing)
   - language weights skewed toward python (80%) and sql (via python)
   - Same date range as narrative.yaml
3. Create `notebooks/meridian_direct/insert.py` that runs the existing
   `direct_data`, `ide_org_level`, `ai_assistant_acceptance` generators
   with `entities_meridian` (orgs[2])
4. Add DevEx filter_values_unity entries pointing to Meridian GitHub repos
5. For SPACE survey: add a Meridian-scoped survey with level_3='demo-meridian'

Copilot adoption story for Meridian: ramps from 15% → 60% over the year,
with the fastest adoption in the post-Opsera phase (engineers now have time to explore AI tools).

---

## Key Constraints & Gotchas

| Constraint | Detail |
|---|---|
| `record_inserted_by` | MUST be `'seed-data-meridian'` (not `'seed-data'`) — deletion of Acme DORA rows uses `'seed-data'` |
| `board_id` | Use `2` for MDP board — Acme uses `1` (raw_jira_boards_ci has no unique constraint but CTFC join expects the board_id to match) |
| `createdBy` (filter_groups) | `'seed-data-meridian@demo.io'` — distinct from Acme's `'seed-data@demo.io'` |
| LTFC very long pre-Opsera | Commit timestamps will be 90–150 days before deploy date. Ensure these fall within or before the start_date (no issue — they're just old commits) |
| CTFC issue cycle times | `issue_updated` must be BETWEEN the date range for the CTFC SQL filter. Use the deploy date as `issue_updated` (resolution date). For pre-Opsera, open date = deploy date - cycle_days |
| `customer_id` for ITSM | Must be `'demo-meridian'` (matches the delete scope) |
| Inflection visibility | The DORA charts aggregate by week/month. Make sure the pre-phase has enough deploys to show bars (at least 2–3 per month) and the post-phase shows a clear ramp |

---

## Expected DORA Chart Appearance After Implementation

**Deployment Frequency**: Sparse bars (2-3/month) for first half of date range, then
visible ramp-up starting Oct 2025 to near-daily by Jan 2026. Clear "before/after" shape.

**Lead Time for Changes**: Starts very high (~120 days average in early months),
drops sharply in Oct-Nov 2025 transition, settles to ~6 days post-Opsera.
This is the most dramatic visual in the story.

**Change Failure Rate**: Starts at ~35%, gradual improvement through transition,
settles to ~8%. The improvement is visible but not as steep as LTFC.

**MTTR**: Starts at 10–15 day average, drops to 4–6 hours in Dec 2025.
Bars in MTTR chart should show tall bars pre-Opsera, then very short bars after.

**Cycle Time (CTFC)**: Pre-Opsera tickets take 60-120 days. Post-Opsera: 3-7 days.
The sine-wave chart should show a clear descending trend with a sharp inflection.
