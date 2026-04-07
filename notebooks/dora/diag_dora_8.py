# Diagnostic round 8: compare our rows vs real rows in pipeline_activities
# to find columns the DF chart might filter on that we're leaving NULL
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_dora_8.py").read())

import json

CATALOG  = "playground_prod"
LEVEL_3  = "demo-acme-corp"
DF_KPI   = "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. Full schema ─────────────────────────────────────────────────────────────
out("pa.schema", [r.asDict() for r in sql(f"DESCRIBE {CATALOG}.base_datasets.pipeline_activities").collect()])

# ── 2. One of our seed rows — all columns ─────────────────────────────────────
out("pa.our_row_full", rows(f"""
    SELECT * FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE record_inserted_by = 'seed-data'
    LIMIT 1
""", 1))

# ── 3. One real github row — all columns ──────────────────────────────────────
out("pa.real_github_row_full", rows(f"""
    SELECT * FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE pipeline_source = 'github'
      AND record_inserted_by != 'seed-data'
    LIMIT 1
""", 1))

# ── 4. Check if DF chart uses account_name / org_name to filter ───────────────
out("pa.our_nullability_check", rows(f"""
    SELECT
        COUNT(*) AS total,
        COUNT(account_name) AS has_account_name,
        COUNT(pipeline_source) AS has_pipeline_source,
        COUNT(project_url) AS has_project_url
    FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE record_inserted_by = 'seed-data'
""", 1))

# ── 5. Look at what real github rows have for account_name / org_name ─────────
out("pa.real_github_account_sample", rows(f"""
    SELECT account_name, pipeline_source, project_url, step_type
    FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE pipeline_source = 'github'
      AND record_inserted_by != 'seed-data'
    LIMIT 3
""", 3))

# ── 6. Check if there's a joining_proj_url computed column or view ─────────────
# Try to see if the DF chart might use a different view/table
out("pa.views_in_base_datasets", [r.asDict() for r in sql(f"""
    SELECT table_name, table_type
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'base_datasets'
      AND table_type = 'VIEW'
""").collect()])
