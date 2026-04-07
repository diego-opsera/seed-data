# DORA Seed Data

## Charts and data sources

| Chart | Base table | Generator |
|---|---|---|
| Deployment Frequency | `base_datasets.pipeline_activities` | `dora_charts.py` |
| Lead Time for Changes | `base_datasets.pipeline_activities` | `dora_charts.py` |
| Change Failure Rate | `base_datasets.cfr_mttr_metric_data` | `dora_charts.py` |
| Mean Time to Restore | `base_datasets.cfr_mttr_metric_data` | `dora_charts.py` |
| Cycle Time for Changes | `base_datasets.v_itsm_issues_hist` → `transform_stage.mt_itsm_issues_hist` | `itsm_issues.py` (direct) |
| SDM maturity badges | `consumption_layer.sdm*` | `dora.py` |

CTFC is the only DORA chart that reads Jira issue data. It is populated by the **direct** generator, not the dora generator.

## Filter config

All five charts resolve their data scope through a filter config chain:

```
filter_groups_unity      (org hierarchy: level_3 = 'demo-acme-corp')
        ↓
filter_values_unity      (per-KPI filter rows, e.g. project_url, board_ids)
        ↓
v_filter_group_values_kpi_flattened_unity  (pivoted view used in chart SQL)
```

Each chart SQL has a `{{whereClause}}` placeholder that resolves to
`WHERE kpi_uuids = '<uuid>' AND level_3 = 'demo-acme-corp'` at runtime.

### KPI UUIDs

| Chart | UUID(s) |
|---|---|
| Deployment Frequency | `60aed2f8-1c74-4792-ad51-bf4e5a65f7b9` |
| Lead Time for Changes | `a9337c02-a00e-40ad-9cdc-2d18dfd771c9` |
| Change Failure Rate | `ab9a59ba-a19c-4358-b195-1648797f77c2` |
| Mean Time to Restore | `906f4f2b-a299-4b24-9a24-2330f45dd493` |
| Cycle Time for Changes | `f60d8a58` (overview), `7f0d028a` (sine wave), `c03790e5` (compare), `26e4b366` (tab data), `8b81e5db` / `6b81e5db` (table) |

### Filter rows per chart

**DF + LTFC** (github):
- `project_url` → must match `pipeline_activities.project_url`
- `deployment_stages` → must match `pipeline_activities.step_name` (value: `deploy`)
- `pipeline_status_success` → value: `success`

**CFR + MTTR** (jira):
- `project_name` → must match `cfr_mttr_metric_data.issue_project` (value: `Acme Platform`)

**CTFC** (jira):
- `project_name` → must match `mt_itsm_issues_hist.issue_project` (value: `ACME`)
- `issue_status` → values: `Done`, `done`, `Completed`
- `include_issue_types` → values: `Story`, `story`, `Bug`, `bug`, `Task`, `task`
- `board_ids` → must match `board_info[].board_id` on issues (value: `1`)
- `defect_type` → values: `Bug`, `bug`

The CTFC chart also requires a matching row in `source_to_stage.raw_jira_boards_ci`
(board_id=1) since issues join against the `jira_boards` view.

## Running the scripts

### Full data wipe and reload

Run both pairs in order:

```python
# 1. Wipe direct data (Jira issues, pull requests, etc.)
exec(open("/tmp/seed-data/notebooks/direct/delete.py").read())

# 2. Wipe DORA data (pipeline events, sdm tables, filter config)
exec(open("/tmp/seed-data/notebooks/dora/delete.py").read())

# 3. Reload direct data (populates mt_itsm_issues_current + mt_itsm_issues_hist)
exec(open("/tmp/seed-data/notebooks/direct/insert.py").read())

# 4. Reload DORA data (populates pipeline events, sdm tables, filter config)
exec(open("/tmp/seed-data/notebooks/dora/insert.py").read())
```

### DORA-only refresh (DF / LTFC / CFR / MTTR only)

If Jira issue data is intact and only pipeline/sdm data needs refreshing:

```python
exec(open("/tmp/seed-data/notebooks/dora/delete.py").read())
exec(open("/tmp/seed-data/notebooks/dora/insert.py").read())
```

CTFC will continue working as long as `mt_itsm_issues_hist` still has direct seed data.

## Key implementation notes

- `pipeline_activities` rows must have `step_name = 'deploy'` (lowercase) and `pipeline_status = 'success'` for DF/LTFC to count them as deployments.
- `mt_itsm_issues_hist` (not `_current`) is what the CTFC chart queries. The `itsm_issues.py` generator writes to both tables.
- `board_info` on Jira issues must contain `board_id = 1` (as a bigint) to pass the `arrays_overlap` join in the CTFC chart SQL.
- Filter config rows are scoped by `created_by = 'seed-data@demo.io'` and `createdBy = 'seed-data@demo.io'` for safe targeted deletion.
