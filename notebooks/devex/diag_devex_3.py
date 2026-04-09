# Diagnostic: debug Commit Statistics "No data available"
# Run: exec(open("/tmp/seed-data/notebooks/devex/diag_devex_3.py").read())

import json

CATALOG = "playground_prod"
ORG     = "demo-acme-direct"
COMMIT_STATS_UUID = "9fd5ec78-9fce-49a0-8154-24d3109d3f05"

FVU  = f"{CATALOG}.master_data.filter_values_unity"
FGVF = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"
CM   = f"{CATALOG}.base_datasets.commits_rest_api"

def sql(q):
    return spark.sql(q)

def rows(q, limit=20):
    return [r.asDict() for r in sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

# 1. DESCRIBE the flattened view to find actual column names and types
out("fgvf.schema", {r["col_name"]: r["data_type"]
    for r in spark.sql(f"DESCRIBE {FGVF}").collect()
    if r["col_name"] and not r["col_name"].startswith("#")})

# 2. FVU: kpi_uuids is ARRAY<STRING> — use ARRAY_CONTAINS
out("fvu.commit_stats_uuid_present", rows(f"""
    SELECT filter_group_id, tool_type, filter_name, filter_values,
           SIZE(kpi_uuids) AS kpi_count, created_by
    FROM {FVU}
    WHERE created_by = 'seed-data@devex.io'
      AND ARRAY_CONTAINS(kpi_uuids, '{COMMIT_STATS_UUID}')
"""))

# 3. Sample commits — do they have .git URLs?
out("commits.sample_project_urls", rows(f"""
    SELECT DISTINCT project_url, COUNT(*) AS n
    FROM {CM}
    WHERE org_name = '{ORG}'
    GROUP BY project_url
"""))

# 4. FGVF: kpi_uuids is STRING (view already exploded) — use =
out("fgvf.exploded_urls_for_commit_stats", rows(f"""
    SELECT DISTINCT exploded_project_url AS project_url
    FROM {FGVF}
    LATERAL VIEW explode_outer(project_url) AS exploded_project_url
    WHERE kpi_uuids = '{COMMIT_STATS_UUID}'
      AND level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
      AND exploded_project_url IS NOT NULL
"""))

# 5. Does the join between commits and filter produce rows?
out("commits.join_to_commit_stats_filter", rows(f"""
    WITH filter AS (
        SELECT DISTINCT exploded_project_url AS project_url
        FROM {FGVF}
        LATERAL VIEW explode_outer(project_url) AS exploded_project_url
        WHERE kpi_uuids = '{COMMIT_STATS_UUID}'
          AND level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
          AND exploded_project_url IS NOT NULL
    )
    SELECT c.project_url, COUNT(*) AS n
    FROM {CM} c INNER JOIN filter f ON c.project_url = f.project_url
    WHERE c.org_name = '{ORG}'
    GROUP BY c.project_url
"""))

# 6. Check before_sha format (affects :mergeCommitOperator filter)
out("commits.before_sha_size_check", rows(f"""
    SELECT SIZE(SPLIT(before_sha, ',')) AS sha_size, COUNT(*) AS n
    FROM {CM}
    WHERE org_name = '{ORG}'
    GROUP BY sha_size
"""))

# 7. Simulate commit_statistics_overview query (simplified, >= 1 operator)
out("commit_stats.simulate_overview_ge1", rows(f"""
    WITH filter AS (
        SELECT DISTINCT exploded_project_url AS project_url
        FROM {FGVF}
        LATERAL VIEW explode_outer(project_url) AS exploded_project_url
        WHERE kpi_uuids = '{COMMIT_STATS_UUID}'
          AND level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
          AND exploded_project_url IS NOT NULL
    )
    SELECT COUNT(DISTINCT c.commit_id) AS commit_count
    FROM {CM} c INNER JOIN filter f ON c.project_url = f.project_url
    WHERE SIZE(SPLIT(c.before_sha, ',')) >= 1
"""))
