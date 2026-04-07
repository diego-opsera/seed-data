# CTFC round 6: test alternative join paths and date range behavior
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_6.py").read())

import json

CATALOG  = "playground_prod"
CTFC_KPI = "adca3119-2b97-4163-831d-ce0f3d150c2f"
LEVEL_3  = "demo-acme-corp"
PROJ_URL = "https://github.com/demo-acme/project_001.git"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def count(q): return sql(q).collect()[0][0]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. What dates do our pdc rows have? ────────────────────────────────────────
out("pdc.date_range", [r.asDict() for r in sql(f"""
    SELECT MIN(committed_date) AS min_committed,
           MAX(committed_date) AS max_committed,
           COUNT(*) AS n
    FROM {CATALOG}.base_datasets.pipeline_deployment_commits
    WHERE record_inserted_by = 'seed-data'
""").collect()])

# ── 2. Try joining pdc via web_url pattern to project_url ─────────────────────
out("ctfc.weburl_join", rows(f"""
    SELECT pdc.committed_date, pdc.commit_id,
           f.project_url_exploded
    FROM {CATALOG}.base_datasets.pipeline_deployment_commits pdc
    JOIN (
        SELECT DISTINCT regexp_replace(exploded_url, '\\.git$', '') AS project_url_exploded
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        LATERAL VIEW EXPLODE(project_url) AS exploded_url
        WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
    ) f ON pdc.web_url LIKE CONCAT(f.project_url_exploded, '/commit/%')
""", 3))

# ── 3. Full simulated CTFC chain: pdc → pa → filter ──────────────────────────
out("ctfc.full_chain_sample", rows(f"""
    SELECT pdc.committed_date, pa.pipeline_started_at,
           datediff(pa.pipeline_started_at, pdc.committed_date) AS cycle_days
    FROM {CATALOG}.base_datasets.pipeline_deployment_commits pdc
    JOIN {CATALOG}.base_datasets.pipeline_activities pa
      ON pdc.`to` = pa.pipeline_commit_sha
    JOIN (
        SELECT DISTINCT exploded_url AS project_url
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        LATERAL VIEW EXPLODE(project_url) AS exploded_url
        WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
    ) f ON pa.project_url = f.project_url
           OR pa.project_url = CONCAT(f.project_url, '.git')
           OR CONCAT(pa.project_url, '.git') = f.project_url
""", 5))

# ── 4. Check what real CTFC data looks like for the Insights org ──────────────
out("pdc.insights_ctfc_sample", rows(f"""
    SELECT pdc.committed_date, pa.pipeline_started_at, pa.pipeline_commit_sha,
           pdc.web_url,
           datediff(pa.pipeline_started_at, pdc.committed_date) AS cycle_days
    FROM {CATALOG}.base_datasets.pipeline_deployment_commits pdc
    JOIN {CATALOG}.base_datasets.pipeline_activities pa
      ON pdc.`to` = pa.pipeline_commit_sha
    WHERE pdc.record_inserted_by != 'seed-data'
      AND pa.pipeline_source = 'github'
    LIMIT 3
""", 3))
