# CTFC round 5: verify pdc data and join chain post-insert
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_5.py").read())

import json

CATALOG  = "playground_prod"
CTFC_KPI = "adca3119-2b97-4163-831d-ce0f3d150c2f"
LEVEL_3  = "demo-acme-corp"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def count(q): return sql(q).collect()[0][0]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. pdc row count ───────────────────────────────────────────────────────────
out("pdc.seed_count", count(f"""
    SELECT COUNT(*) FROM {CATALOG}.base_datasets.pipeline_deployment_commits
    WHERE record_inserted_by = 'seed-data'
"""))

# ── 2. Sample pdc rows ─────────────────────────────────────────────────────────
out("pdc.sample", rows(f"""
    SELECT pipeline_source, `from`, `to`, repository_id, committed_date,
           commit_id, owner, web_url, record_inserted_by
    FROM {CATALOG}.base_datasets.pipeline_deployment_commits
    WHERE record_inserted_by = 'seed-data'
""", 2))

# ── 3. PA rows with pipeline_commit_sha set ────────────────────────────────────
out("pa.with_commit_sha_count", count(f"""
    SELECT COUNT(*) FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE record_inserted_by = 'seed-data'
      AND pipeline_commit_sha IS NOT NULL
"""))

out("pa.commit_sha_sample", rows(f"""
    SELECT pipeline_commit_sha, pipeline_started_at, pipeline_status
    FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE record_inserted_by = 'seed-data'
      AND pipeline_commit_sha IS NOT NULL
""", 2))

# ── 4. The actual join ─────────────────────────────────────────────────────────
out("ctfc.join_sample", rows(f"""
    SELECT pa.pipeline_started_at, pa.pipeline_commit_sha,
           pdc.committed_date, pdc.commit_id,
           datediff(pa.pipeline_started_at, pdc.committed_date) AS cycle_days
    FROM {CATALOG}.base_datasets.pipeline_activities pa
    JOIN {CATALOG}.base_datasets.pipeline_deployment_commits pdc
      ON pa.pipeline_commit_sha = pdc.`to`
    WHERE pa.record_inserted_by = 'seed-data'
""", 3))

# ── 5. View for CTFC KPI ───────────────────────────────────────────────────────
out("view.ctfc_row", rows(f"""
    SELECT level_3, kpi_uuids, tool_type, project_url
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
""", 2))
