# CTFC round 3: inspect pipeline_deployment_commits and find CTFC KPI UUID
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_3.py").read())

import json

CATALOG  = "playground_prod"
LTFC_KPI = "a9337c02-a00e-40ad-9cdc-2d18dfd771c9"
DF_KPI   = "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. pipeline_deployment_commits schema ──────────────────────────────────────
out("pdc.schema", [r.asDict() for r in sql(f"DESCRIBE {CATALOG}.base_datasets.pipeline_deployment_commits").collect()])

# ── 2. Sample real rows ────────────────────────────────────────────────────────
out("pdc.sample", rows(f"SELECT * FROM {CATALOG}.base_datasets.pipeline_deployment_commits", 2))

# ── 3. Which KPI UUIDs are in filter_values_unity for the Insights Eng org
#       that are NOT DF/LTFC/CFR/MTTR — paired with their filter_name ──────────
out("fvu.insights_non_dora_rows", [r.asDict() for r in sql(f"""
    SELECT filter_group_id, tool_type, filter_name, filter_values, kpi_uuids
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE filter_group_id = '14002bd3-35c6-4669-98e0-98deb8e0e2f9'
      AND NOT array_contains(kpi_uuids, '{DF_KPI}')
      AND NOT array_contains(kpi_uuids, '{LTFC_KPI}')
""").collect()])

# ── 4. What view columns does the LTFC KPI produce for Insights Eng? ──────────
out("view.ltfc_row", rows(f"""
    SELECT * FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE filter_group_id = '14002bd3-35c6-4669-98e0-98deb8e0e2f9'
      AND kpi_uuids = '{LTFC_KPI}'
""", 1))
