# Find CTFC KPI UUID and understand what data it needs
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc.py").read())

import json

CATALOG  = "playground_prod"
OUR_FGID = "6b35e559-cde7-4c79-b3c2-891589f706fa"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. Find CTFC / Cycle Time KPI UUIDs ───────────────────────────────────────
out("kpis.cycle_time_related", [r.asDict() for r in sql(f"""
    SELECT DISTINCT kpi_uuid
    FROM {CATALOG}.master_data.filter_values_unity
    LATERAL VIEW EXPLODE(kpi_uuids) AS kpi_uuid
    WHERE kpi_uuid NOT IN (
        '60aed2f8-1c74-4792-ad51-bf4e5a65f7b9',
        'a9337c02-a00e-40ad-9cdc-2d18dfd771c9',
        'ab9a59ba-a19c-4358-b195-1648797f77c2',
        '906f4f2b-a299-4b24-9a24-2330f45dd493'
    )
    AND filter_group_id != '{OUR_FGID}'
""").limit(20).collect()])

# ── 2. What KPI UUIDs does the real Insights Engineering org use? ─────────────
out("kpis.insights_engineering_all", [r.asDict() for r in sql(f"""
    SELECT DISTINCT kpi_uuid
    FROM {CATALOG}.master_data.filter_values_unity
    LATERAL VIEW EXPLODE(kpi_uuids) AS kpi_uuid
    WHERE filter_group_id = '14002bd3-35c6-4669-98e0-98deb8e0e2f9'
""").collect()])

# ── 3. What tables exist in consumption_layer with ctfc / cycle? ───────────────
out("consumption.ctfc_tables", [r.asDict() for r in sql(f"""
    SELECT table_name, table_type
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'consumption_layer'
      AND (lower(table_name) LIKE '%ctfc%' OR lower(table_name) LIKE '%cycle%')
""").collect()])
