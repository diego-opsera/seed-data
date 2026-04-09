# Diagnostic: verify filter group wiring and data after re-insert
# Run via: exec(open("/tmp/seed-data/notebooks/devex/diag_devex_2.py").read())

import json

CATALOG = "playground_prod"
ORG     = "demo-acme-direct"

def sql(q):
    return spark.sql(q)

def rows(q, limit=20):
    return [r.asDict() for r in sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

FGU  = f"{CATALOG}.master_data.filter_groups_unity"
FVU  = f"{CATALOG}.master_data.filter_values_unity"
FGVF = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"
PR   = f"{CATALOG}.base_datasets.pull_requests"
CM   = f"{CATALOG}.base_datasets.commits_rest_api"

# ── 1. All filter group rows for seed data ───────────────────────────────────
out("filter_groups_unity.all_seed_rows", rows(f"""
    SELECT id, filter_group_id, level_1, level_2, level_3, createdBy
    FROM {FGU}
    WHERE createdBy IN ('seed-data@demo.io', 'seed-data@devex.io')
"""))

# ── 2. All filter values for seed data ───────────────────────────────────────
out("filter_values_unity.all_seed_rows", rows(f"""
    SELECT filter_group_id, tool_type, filter_name, filter_values,
           SIZE(kpi_uuids) AS kpi_count, created_by
    FROM {FVU}
    WHERE created_by IN ('seed-data@demo.io', 'seed-data@devex.io')
    ORDER BY created_by, filter_name
"""))

# ── 3. What does the flattened view return for seed data? ─────────────────────
out("fgvf.schema", {r["col_name"]: r["data_type"]
    for r in spark.sql(f"DESCRIBE {FGVF}").collect()
    if r["col_name"] and not r["col_name"].startswith("#")})

out("fgvf.all_seed_rows", rows(f"""
    SELECT level_1, level_2, level_3, project_url, project_name,
           team_names, board_ids, issue_status, include_issue_types
    FROM {FGVF}
    WHERE lower(concat_ws(' ', level_1, level_2, level_3)) RLIKE 'acme|demo'
    LIMIT 20
"""))

# ── 4. Simulate filter CTE for developer_throughput (whereClause by level) ───
# The dashboard is filtered on level_1='Acme Corp', level_3='demo-acme-corp'
out("fgvf.simulate_whereClause_by_level", rows(f"""
    SELECT project_url, project_name, team_names, board_ids,
           issue_status, include_issue_types
    FROM {FGVF}
    WHERE level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
"""))

# ── 5. Does the filter CTE produce project_url rows? ─────────────────────────
out("fgvf.exploded_project_urls_for_devex", rows(f"""
    SELECT DISTINCT exploded_project_url AS project_url
    FROM {FGVF}
    LATERAL VIEW explode_outer(project_url) AS exploded_project_url
    WHERE level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
    AND exploded_project_url IS NOT NULL
"""))

# ── 6. Does that join to commits? ─────────────────────────────────────────────
out("commits.join_to_filter", rows(f"""
    WITH filter AS (
        SELECT DISTINCT exploded_project_url AS project_url
        FROM {FGVF}
        LATERAL VIEW explode_outer(project_url) AS exploded_project_url
        WHERE level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
        AND exploded_project_url IS NOT NULL
    )
    SELECT c.project_url, COUNT(*) AS n
    FROM {CM} c INNER JOIN filter f ON c.project_url = f.project_url
    GROUP BY c.project_url
"""))

# ── 7. Does that join to PRs? ─────────────────────────────────────────────────
out("prs.join_to_filter", rows(f"""
    WITH filter AS (
        SELECT DISTINCT exploded_project_url AS project_url
        FROM {FGVF}
        LATERAL VIEW explode_outer(project_url) AS exploded_project_url
        WHERE level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
        AND exploded_project_url IS NOT NULL
    )
    SELECT pr.project_url, COUNT(*) AS n
    FROM {PR} pr INNER JOIN filter f ON pr.project_url = f.project_url
    GROUP BY pr.project_url
"""))

# ── 8. Check PR fields needed for developer throughput metrics ────────────────
out("prs.throughput_fields_sample", rows(f"""
    WITH filter AS (
        SELECT DISTINCT exploded_project_url AS project_url
        FROM {FGVF}
        LATERAL VIEW explode_outer(project_url) AS exploded_project_url
        WHERE level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
        AND exploded_project_url IS NOT NULL
    )
    SELECT pr.project_url, pr.pr_state,
           pr.pr_created_datetime,
           pr.pr_merged_datetime,
           pr.first_pr_review_submitted_datetime,
           pr.first_commit_id,
           SIZE(pr.pr_commits) AS commits_count,
           pr.pr_commits[0].commit_timestamp AS first_commit_ts_in_array
    FROM {PR} pr INNER JOIN filter f ON pr.project_url = f.project_url
    ORDER BY pr.pr_created_datetime DESC
""", 5))

# ── 9. Commits raw data check ─────────────────────────────────────────────────
out("commits.sample_after_reinsert", rows(f"""
    SELECT project_url, commit_date, has_ticket_id, before_sha
    FROM {CM}
    WHERE org_name = '{ORG}'
    ORDER BY commit_date DESC
""", 5))
