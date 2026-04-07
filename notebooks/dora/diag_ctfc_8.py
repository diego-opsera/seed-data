# CTFC round 8: find the Jira issue base table used by CTFC chart
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_8.py").read())

import json

CATALOG       = "playground_prod"
CTFC_KPI      = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"
LEVEL_3       = "demo-acme-corp"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. List all tables in base_datasets ───────────────────────────────────────
out("base_datasets.tables", [r.asDict() for r in sql(f"""
    SELECT table_name FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'base_datasets'
    ORDER BY table_name
""").collect()])

# ── 2. Look for jira-related tables in any schema ─────────────────────────────
out("all.jira_tables", [r.asDict() for r in sql(f"""
    SELECT table_schema, table_name FROM {CATALOG}.information_schema.tables
    WHERE lower(table_name) LIKE '%jira%'
       OR lower(table_name) LIKE '%issue%'
       OR lower(table_name) LIKE '%ticket%'
    ORDER BY table_schema, table_name
""").collect()])

# ── 3. What consumption_layer tables exist for CTFC? ──────────────────────────
out("consumption_layer.ctfc_tables", [r.asDict() for r in sql(f"""
    SELECT table_name FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'consumption_layer'
      AND lower(table_name) LIKE '%ctfc%'
    ORDER BY table_name
""").collect()])

# ── 4. Check sdm_ctfc schema to understand source ────────────────────────────
out("sdm_ctfc.schema", [r.asDict() for r in sql(f"DESCRIBE {CATALOG}.consumption_layer.sdm_ctfc").collect()])

# ── 5. Sample real sdm_ctfc data (non-seed) ───────────────────────────────────
out("sdm_ctfc.real_sample", rows(f"""
    SELECT * FROM {CATALOG}.consumption_layer.sdm_ctfc
    WHERE level != '{LEVEL_3}'
    LIMIT 2
""", 2))
