# Diagnostic round 6: verify data is present and the join chain works end-to-end
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_dora_6.py").read())

import json

CATALOG = "playground_prod"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def count(q): return sql(q).collect()[0][0]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

PROJECT_URL   = "https://github.com/demo-acme/project_001.git"
ISSUE_PROJECT = "Acme Platform"
LEVEL_3       = "demo-acme-corp"

DF_KPI   = "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9"
LTFC_KPI = "a9337c02-a00e-40ad-9cdc-2d18dfd771c9"
CFR_KPI  = "ab9a59ba-a19c-4358-b195-1648797f77c2"
MTTR_KPI = "906f4f2b-a299-4b24-9a24-2330f45dd493"

# ── 1. Is our data in the base tables? ───────────────────────────────────────
out("pa.row_count_seed_data",
    count(f"SELECT COUNT(*) FROM {CATALOG}.base_datasets.pipeline_activities WHERE record_inserted_by = 'seed-data'"))

out("pa.sample_seed_rows", rows(f"""
    SELECT project_url, pipeline_status, step_type, pipeline_started_at, record_inserted_by
    FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE record_inserted_by = 'seed-data'
""", 3))

out("cfr.row_count_seed_data",
    count(f"SELECT COUNT(*) FROM {CATALOG}.base_datasets.cfr_mttr_metric_data WHERE record_inserted_by = 'seed-data'"))

out("cfr.sample_seed_rows", rows(f"""
    SELECT issue_key, issue_project, issue_status, itsm_source,
           cfr_total_changes_key, cfr_total_failures_key, mttr_issue_key,
           issue_resolution_date
    FROM {CATALOG}.base_datasets.cfr_mttr_metric_data
    WHERE record_inserted_by = 'seed-data'
""", 3))

# ── 2. Does filter_groups_unity have our hierarchy entry? ────────────────────
out("fgu.demo_acme_corp_rows", rows(f"""
    SELECT id, level_1, level_2, level_3, filter_group_id, active
    FROM {CATALOG}.master_data.filter_groups_unity
    WHERE level_3 = '{LEVEL_3}' OR level_1 = 'Acme Corp'
""", 5))

# ── 3. Does filter_values_unity have our project_url / project_name entries? ─
out("fvu.seed_data_rows", rows(f"""
    SELECT filter_group_id, tool_type, filter_name, filter_values, kpi_uuids, active
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE created_by = 'seed-data@demo.io'
""", 5))

# ── 4. What filter_values_unity rows are linked to our filter_group_id? ──────
# First get our filter_group_id
our_fgids = [r["filter_group_id"] for r in
             sql(f"SELECT filter_group_id FROM {CATALOG}.master_data.filter_groups_unity WHERE level_3 = '{LEVEL_3}'").collect()]
out("our_filter_group_ids", our_fgids)

if our_fgids:
    fgid = our_fgids[0]
    out("fvu.rows_for_our_fgid", rows(f"""
        SELECT filter_group_id, tool_type, filter_name, filter_values, kpi_uuids, active
        FROM {CATALOG}.master_data.filter_values_unity
        WHERE filter_group_id = '{fgid}'
    """, 10))

# ── 5. Does the view return anything for our level_3 + DF KPI? ───────────────
out("view.df_for_demo_acme_corp", rows(f"""
    SELECT level_1, level_3, filter_group_id, kpi_uuids, tool_type, project_url
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = '{LEVEL_3}'
      AND kpi_uuids = '{DF_KPI}'
""", 5))

out("view.cfr_for_demo_acme_corp", rows(f"""
    SELECT level_1, level_3, filter_group_id, kpi_uuids, tool_type, project_name
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = '{LEVEL_3}'
      AND kpi_uuids = '{CFR_KPI}'
""", 5))

# ── 6. Simulate the DF filter CTE join ───────────────────────────────────────
out("df_join_simulation", rows(f"""
    SELECT pa.project_url, pa.pipeline_status, pa.pipeline_started_at
    FROM {CATALOG}.base_datasets.pipeline_activities pa
    JOIN (
        SELECT DISTINCT exploded_project_url AS project_url
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        LATERAL VIEW EXPLODE(project_url) AS exploded_project_url
        WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{DF_KPI}'
    ) f ON pa.project_url = f.project_url
    WHERE pa.pipeline_status IN ('success', 'failed')
""", 5))

# ── 7. Simulate the CFR filter CTE join ──────────────────────────────────────
out("cfr_join_simulation", rows(f"""
    SELECT s.issue_key, s.issue_project, s.cfr_total_changes_key, s.cfr_total_failures_key
    FROM {CATALOG}.base_datasets.cfr_mttr_metric_data s
    JOIN (
        SELECT DISTINCT COALESCE(exploded_project_name, 'x') AS project_name
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        LATERAL VIEW EXPLODE_OUTER(project_name) AS exploded_project_name
        WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CFR_KPI}'
    ) f ON s.issue_project = f.project_name
    WHERE s.itsm_source = 'jira'
      AND s.issue_resolution_date IS NOT NULL
""", 5))
