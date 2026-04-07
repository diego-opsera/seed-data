# CTFC round 7: identify correct KPI UUID and source table for Cycle Time chart
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_7.py").read())

import json

CATALOG       = "playground_prod"
CTFC_CAND_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. sprint_data schema ──────────────────────────────────────────────────────
out("sprint_data.schema", [r.asDict() for r in sql(f"DESCRIBE {CATALOG}.base_datasets.sprint_data").collect()])

# ── 2. sample sprint_data row ──────────────────────────────────────────────────
out("sprint_data.sample", rows(f"SELECT * FROM {CATALOG}.base_datasets.sprint_data", 1))

# ── 3. What filter_values_unity rows exist for the CTFC candidate KPI? ─────────
out("fvu.ctfc_cand_rows", [r.asDict() for r in sql(f"""
    SELECT filter_group_id, tool_type, filter_name, filter_values, kpi_uuids
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE array_contains(kpi_uuids, '{CTFC_CAND_KPI}')
    LIMIT 5
""").collect()])

# ── 4. What does the view return for CTFC candidate KPI? ─────────────────────
out("view.ctfc_cand_sample", rows(f"""
    SELECT level_3, kpi_uuids, tool_type, project_name, board_ids, issue_status, include_issue_types
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE kpi_uuids = '{CTFC_CAND_KPI}'
    LIMIT 3
""", 3))
