# Diagnostic round 7: verify pipeline_activities data and DF join post-insert
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_dora_7.py").read())

import json

CATALOG   = "playground_prod"
LEVEL_3   = "demo-acme-corp"
DF_KPI    = "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9"
PROJ_URL  = "https://github.com/demo-acme/project_001.git"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def count(q): return sql(q).collect()[0][0]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. Row counts ──────────────────────────────────────────────────────────────
out("pa.total_seed_rows",
    count(f"SELECT COUNT(*) FROM {CATALOG}.base_datasets.pipeline_activities WHERE record_inserted_by = 'seed-data'"))

# ── 2. Sample rows — check pipeline_source, step_type, branch ─────────────────
out("pa.sample", rows(f"""
    SELECT pipeline_source, project_url, pipeline_status, step_type, step_status,
           step_conclusion, branch, pipeline_started_at
    FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE record_inserted_by = 'seed-data'
    ORDER BY pipeline_started_at
""", 3))

# ── 3. Distinct values of fields that might be filtered by the DF chart ────────
out("pa.distinct_pipeline_source",
    [r.asDict() for r in sql(f"""
        SELECT pipeline_source, COUNT(*) AS n
        FROM {CATALOG}.base_datasets.pipeline_activities
        WHERE record_inserted_by = 'seed-data'
        GROUP BY 1
    """).collect()])

out("pa.distinct_step_type",
    [r.asDict() for r in sql(f"""
        SELECT step_type, COUNT(*) AS n
        FROM {CATALOG}.base_datasets.pipeline_activities
        WHERE record_inserted_by = 'seed-data'
        GROUP BY 1
    """).collect()])

# ── 4. Check what step_type values exist in the FULL table (real data) ─────────
out("pa.real_data_distinct_step_type",
    [r.asDict() for r in sql(f"""
        SELECT step_type, COUNT(*) AS n
        FROM {CATALOG}.base_datasets.pipeline_activities
        WHERE record_inserted_by != 'seed-data'
        GROUP BY 1
        ORDER BY n DESC
    """).limit(10).collect()])

out("pa.real_data_distinct_pipeline_source",
    [r.asDict() for r in sql(f"""
        SELECT pipeline_source, COUNT(*) AS n
        FROM {CATALOG}.base_datasets.pipeline_activities
        WHERE record_inserted_by != 'seed-data'
        GROUP BY 1
        ORDER BY n DESC
    """).limit(10).collect()])

# ── 5. DF join simulation now that data exists ─────────────────────────────────
out("df_join_simulation", rows(f"""
    SELECT pa.project_url, pa.pipeline_source, pa.pipeline_status,
           pa.step_type, pa.pipeline_started_at
    FROM {CATALOG}.base_datasets.pipeline_activities pa
    JOIN (
        SELECT DISTINCT exploded_project_url AS project_url
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        LATERAL VIEW EXPLODE(project_url) AS exploded_project_url
        WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{DF_KPI}'
    ) f ON pa.project_url = f.project_url
    WHERE pa.pipeline_status IN ('success', 'failed')
""", 5))

# ── 6. Direct URL match (no view) ─────────────────────────────────────────────
out("pa.direct_url_match",
    count(f"""
        SELECT COUNT(*) FROM {CATALOG}.base_datasets.pipeline_activities
        WHERE project_url = '{PROJ_URL}'
          AND record_inserted_by = 'seed-data'
    """))

# ── 7. What does the view return for project_url after EXPLODE? ────────────────
out("view.exploded_project_url", rows(f"""
    SELECT DISTINCT exploded_project_url
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    LATERAL VIEW EXPLODE(project_url) AS exploded_project_url
    WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{DF_KPI}'
""", 5))
