# CTFC round 10: find demo-acme-corp filter_group_id and simulate CTFC join
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_10.py").read())

import json

CATALOG  = "playground_prod"
CTFC_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"
LEVEL_3  = "demo-acme-corp"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def count(q): return sql(q).collect()[0][0]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. Demo-acme-corp filter_group_id ─────────────────────────────────────────
out("fgu.acme_corp_row", [r.asDict() for r in sql(f"""
    SELECT filter_group_id, customer_id, level_3
    FROM {CATALOG}.master_data.filter_groups_unity
    WHERE level_3 = '{LEVEL_3}'
    LIMIT 3
""").collect()])

# ── 2. Does the view already have a row for CTFC KPI + demo-acme-corp? ────────
out("view.acme_ctfc_row", [r.asDict() for r in sql(f"""
    SELECT level_3, kpi_uuids, tool_type, project_name, board_ids, issue_status, include_issue_types
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
""").collect()])

# ── 3. What distinct issue_project values do our issues have? ─────────────────
out("itsm.acme_projects", [r.asDict() for r in sql(f"""
    SELECT DISTINCT issue_project, issue_status, issue_type
    FROM {CATALOG}.base_datasets.v_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
    LIMIT 20
""").collect()])

# ── 4. Count our issues by status ─────────────────────────────────────────────
out("itsm.status_counts", [r.asDict() for r in sql(f"""
    SELECT issue_status, COUNT(*) AS n
    FROM {CATALOG}.base_datasets.v_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
    GROUP BY issue_status
    ORDER BY n DESC
""").collect()])

# ── 5. Simulate CTFC filter join (project_name + status) ──────────────────────
# Use known project_name "ACME" and status "done" / "Done"
out("ctfc.sim_join_done_lower", [{"count": count(f"""
    SELECT COUNT(*)
    FROM {CATALOG}.base_datasets.v_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
      AND issue_project = 'ACME'
      AND lower(issue_status) IN ('done', 'completed')
      AND issue_resolution_date IS NOT NULL
""")}])

# ── 6. Check board_ids column in fvu for Insights org (real working example) ──
out("fvu.insights_board_ids_row", [r.asDict() for r in sql(f"""
    SELECT filter_group_id, filter_name, filter_values, kpi_uuids
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE array_contains(kpi_uuids, '{CTFC_KPI}')
      AND filter_name = 'board_ids'
    LIMIT 2
""").collect()])
