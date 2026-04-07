# CTFC round 4: understand how pdc joins to filter config
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_4.py").read())

import json

CATALOG  = "playground_prod"
CTFC_KPI = "adca3119-2b97-4163-831d-ce0f3d150c2f"
INSIGHTS_FGID = "14002bd3-35c6-4669-98e0-98deb8e0e2f9"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. What does the view return for the Insights org + CTFC KPI? ──────────────
out("view.insights_ctfc", rows(f"""
    SELECT * FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE filter_group_id = '{INSIGHTS_FGID}' AND kpi_uuids = '{CTFC_KPI}'
""", 1))

# ── 2. How does pdc look for one of the Insights org repos? ───────────────────
# Try to find pdc rows for a known Insights repo
out("pdc.insights_repo_sample", rows(f"""
    SELECT pipeline_source, `from`, `to`, repository_id, committed_date,
           commit_id, owner, web_url, record_inserted_by
    FROM {CATALOG}.base_datasets.pipeline_deployment_commits
    WHERE web_url LIKE '%OpseraEngineering/vnxt-insights-api%'
""", 3))

# ── 3. How many pdc rows exist per owner? ─────────────────────────────────────
out("pdc.distinct_owners", [r.asDict() for r in sql(f"""
    SELECT owner, COUNT(*) AS n
    FROM {CATALOG}.base_datasets.pipeline_deployment_commits
    GROUP BY 1
    ORDER BY n DESC
""").limit(10).collect()])

# ── 4. Can we join pdc to pipeline_activities via commit SHA? ──────────────────
# Check if pipeline_activities.pipeline_commit_sha matches pdc.to for Insights
out("pdc_pa.sha_join_sample", rows(f"""
    SELECT pa.pipeline_started_at, pa.pipeline_commit_sha,
           pdc.committed_date, pdc.commit_id, pdc.`from`, pdc.`to`
    FROM {CATALOG}.base_datasets.pipeline_activities pa
    JOIN {CATALOG}.base_datasets.pipeline_deployment_commits pdc
      ON pa.pipeline_commit_sha = pdc.`to`
    WHERE pa.pipeline_source = 'github'
""", 3))

# ── 5. What does pdc.repository_id look like vs project_url? ─────────────────
# Check if repository_id can be extracted from project_url somehow
out("pdc.distinct_pipeline_sources", [r.asDict() for r in sql(f"""
    SELECT pipeline_source, COUNT(*) AS n
    FROM {CATALOG}.base_datasets.pipeline_deployment_commits
    GROUP BY 1
""").collect()])
