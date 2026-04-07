# CTFC round 11: verify filter rows, board_info update, and simulate chart join
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_11.py").read())

import json

CATALOG  = "playground_prod"
CTFC_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"
LEVEL_3  = "demo-acme-corp"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def count(q): return sql(q).collect()[0][0]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. Did the filter rows get inserted? ──────────────────────────────────────
out("fvu.ctfc_acme_corp_rows", [r.asDict() for r in sql(f"""
    SELECT filter_name, filter_values, tool_type
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE filter_group_id = 'd277535f-a8cb-4429-965d-a9de685b4045'
      AND array_contains(kpi_uuids, '{CTFC_KPI}')
""").collect()])

# ── 2. Does the view now have the demo-acme-corp CTFC row? ───────────────────
out("view.acme_corp_ctfc", [r.asDict() for r in sql(f"""
    SELECT level_3, kpi_uuids, project_name, board_ids, issue_status
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
    LIMIT 1
""").collect()])

# ── 3. Did board_info get updated? ────────────────────────────────────────────
out("itsm.board_info_updated", [r.asDict() for r in sql(f"""
    SELECT SIZE(board_info) AS board_info_size, board_info
    FROM {CATALOG}.transform_stage.mt_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
    LIMIT 2
""").collect()])

# ── 4. Count issues where board_info is populated ────────────────────────────
out("itsm.board_info_count", [{"with_board": count(f"""
    SELECT COUNT(*) FROM {CATALOG}.transform_stage.mt_itsm_issues_current
    WHERE record_inserted_by = 'seed-data' AND SIZE(board_info) > 0
"""), "total": count(f"""
    SELECT COUNT(*) FROM {CATALOG}.transform_stage.mt_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
""")}])

# ── 5. Simulate chart join: issues × filter (project_name + issue_status) ────
out("ctfc.sim_join_via_filter", rows(f"""
    SELECT i.issue_key, i.issue_project, i.issue_status, i.issue_type,
           i.issue_created, i.issue_resolution_date,
           bi.board_id, bi.board_name,
           datediff(i.issue_resolution_date, i.issue_created) AS cycle_days
    FROM {CATALOG}.base_datasets.v_itsm_issues_current i
    LATERAL VIEW EXPLODE(i.board_info) AS bi
    JOIN (
        SELECT project_name, issue_status, board_ids
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
        LIMIT 1
    ) f ON array_contains(f.project_name, i.issue_project)
       AND array_contains(f.issue_status, i.issue_status)
       AND array_contains(f.board_ids, CAST(bi.board_id AS STRING))
""", 3))

# ── 6. What date range does our data cover? ───────────────────────────────────
out("itsm.date_range", [r.asDict() for r in sql(f"""
    SELECT MIN(issue_created) AS min_created,
           MAX(issue_resolution_date) AS max_resolved,
           COUNT(*) AS n
    FROM {CATALOG}.base_datasets.v_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
      AND lower(issue_status) IN ('done', 'completed')
      AND issue_resolution_date IS NOT NULL
""").collect()])
