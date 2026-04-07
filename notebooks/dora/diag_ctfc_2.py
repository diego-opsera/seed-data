# CTFC round 2: check sdm_ctfc structure and find the source base table
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_2.py").read())

import json

CATALOG  = "playground_prod"
OUR_FGID = "6b35e559-cde7-4c79-b3c2-891589f706fa"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. sdm_ctfc schema and sample ─────────────────────────────────────────────
out("sdm_ctfc.schema", [r.asDict() for r in sql(f"DESCRIBE {CATALOG}.consumption_layer.sdm_ctfc").collect()])

out("sdm_ctfc.sample", rows(f"SELECT * FROM {CATALOG}.consumption_layer.sdm_ctfc LIMIT 2", 2))

# ── 2. What base tables exist that might feed cycle time? ─────────────────────
out("base_datasets.all_tables", [r.asDict() for r in sql(f"""
    SELECT table_name, table_type
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'base_datasets'
      AND table_type = 'MANAGED'
    ORDER BY table_name
""").collect()])

# ── 3. Check sdm_ctfc for the Insights Engineering org (a real working entry) ──
out("sdm_ctfc.insights_engineering_sample", rows(f"""
    SELECT * FROM {CATALOG}.consumption_layer.sdm_ctfc
    WHERE level LIKE '%Insight%' OR level LIKE '%insight%'
    LIMIT 3
""", 3))

# ── 4. What distinct levels exist in sdm_ctfc? ────────────────────────────────
out("sdm_ctfc.distinct_levels", [r.asDict() for r in sql(f"""
    SELECT level, COUNT(*) AS n FROM {CATALOG}.consumption_layer.sdm_ctfc
    GROUP BY 1 ORDER BY n DESC
""").limit(10).collect()])

# ── 5. Is our level in sdm_ctfc? ──────────────────────────────────────────────
out("sdm_ctfc.our_level", rows(f"""
    SELECT * FROM {CATALOG}.consumption_layer.sdm_ctfc
    WHERE level = 'demo-acme-corp'
""", 3))
