# Diagnostic: devex dashboards — pull_requests, commits_rest_api, teams, itsm, filter groups
# Output is JSON/compact — machine-readable for Claude, not pretty-printed tables
# Run via: exec(open("/tmp/seed-data/notebooks/devex/diag_devex_1.py").read())

import json

CATALOG = "playground_prod"
ORG = "demo-acme-direct"

def sql(q):
    return spark.sql(q)

def rows(q, limit=10):
    return [r.asDict() for r in sql(q).limit(limit).collect()]

def schema(table):
    return {r["col_name"]: r["data_type"] for r in sql(f"DESCRIBE {table}").collect()
            if r["col_name"] and not r["col_name"].startswith("#")}

def count(q):
    return sql(q).collect()[0][0]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

# ── pull_requests ────────────────────────────────────────────────────────────

PR = f"{CATALOG}.base_datasets.pull_requests"

out("pr.schema", schema(PR))

out("pr.count_demo", count(f"SELECT COUNT(*) FROM {PR} WHERE org_name = '{ORG}'"))

out("pr.date_range", rows(f"""
    SELECT MIN(TO_DATE(pr_created_datetime)) AS min_date,
           MAX(TO_DATE(pr_created_datetime)) AS max_date,
           COUNT(*) AS n
    FROM {PR} WHERE org_name = '{ORG}'
""", 1))

out("pr.sample_key_fields", rows(f"""
    SELECT project_url, merge_request_url, pr_source, pr_state,
           pr_created_datetime, pr_merged_datetime,
           first_pr_review_submitted_datetime, first_pr_approved_timestamp,
           pr_user_id, first_commit_id,
           lines_added, lines_removed,
           fix_versions, board_ids
    FROM {PR} WHERE org_name = '{ORG}'
    ORDER BY pr_created_datetime DESC
""", 5))

# Check pr_commits array — are commit_timestamp fields populated?
out("pr.pr_commits_sample", rows(f"""
    SELECT merge_request_url,
           SIZE(pr_commits) AS commits_count,
           pr_commits[0].commit_timestamp AS first_commit_ts,
           pr_commits[0].sha             AS first_commit_sha_in_array
    FROM {PR} WHERE org_name = '{ORG}' AND pr_commits IS NOT NULL
    ORDER BY pr_created_datetime DESC
""", 5))

out("pr.null_rates_key_fields", rows(f"""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN first_commit_id IS NULL THEN 1 ELSE 0 END) AS null_first_commit_id,
        SUM(CASE WHEN first_pr_review_submitted_datetime IS NULL THEN 1 ELSE 0 END) AS null_review_submitted,
        SUM(CASE WHEN pr_merged_datetime IS NULL THEN 1 ELSE 0 END) AS null_pr_merged,
        SUM(CASE WHEN lines_added IS NULL THEN 1 ELSE 0 END) AS null_lines_added,
        SUM(CASE WHEN pr_commits IS NULL OR SIZE(pr_commits) = 0 THEN 1 ELSE 0 END) AS null_or_empty_pr_commits
    FROM {PR} WHERE org_name = '{ORG}'
""", 1))

out("pr.pr_source_breakdown", rows(f"""
    SELECT pr_source, COUNT(*) AS n FROM {PR}
    WHERE org_name = '{ORG}' GROUP BY pr_source ORDER BY n DESC
""", 10))

# ── commits_rest_api ─────────────────────────────────────────────────────────

CM = f"{CATALOG}.base_datasets.commits_rest_api"

out("commits.schema", schema(CM))

out("commits.count_demo", count(f"SELECT COUNT(*) FROM {CM} WHERE org_name = '{ORG}'"))

out("commits.date_range", rows(f"""
    SELECT MIN(commit_date) AS min_date, MAX(commit_date) AS max_date, COUNT(*) AS n
    FROM {CM} WHERE org_name = '{ORG}'
""", 1))

out("commits.sample_key_fields", rows(f"""
    SELECT commit_id, commit_date, commit_timestamp, user_id, project_url,
           has_ticket_id, before_sha, fix_versions, board_ids
    FROM {CM} WHERE org_name = '{ORG}'
    ORDER BY commit_date DESC
""", 5))

out("commits.null_rates_key_fields", rows(f"""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN commit_id IS NULL THEN 1 ELSE 0 END) AS null_commit_id,
        SUM(CASE WHEN has_ticket_id IS NULL THEN 1 ELSE 0 END) AS null_has_ticket_id,
        SUM(CASE WHEN before_sha IS NULL THEN 1 ELSE 0 END) AS null_before_sha,
        SUM(CASE WHEN SIZE(SPLIT(before_sha, ',')) <= 1 THEN 1 ELSE 0 END) AS non_merge_commits
    FROM {CM} WHERE org_name = '{ORG}'
""", 1))

# before_sha split check — devex uses SIZE(SPLIT(before_sha, ',')) to filter merge commits
out("commits.before_sha_split_dist", rows(f"""
    SELECT SIZE(SPLIT(before_sha, ',')) AS sha_parts, COUNT(*) AS n
    FROM {CM} WHERE org_name = '{ORG}'
    GROUP BY sha_parts ORDER BY n DESC
""", 10))

# Check commit_id linkage to pull_requests.first_commit_id
out("commits.commit_id_matches_pr_first_commit", rows(f"""
    SELECT COUNT(*) AS matching_commit_ids
    FROM {PR} pr
    JOIN {CM} c ON pr.first_commit_id = c.commit_id
    WHERE pr.org_name = '{ORG}' AND c.org_name = '{ORG}'
""", 1))

# ── v_github_teams_members_current ──────────────────────────────────────────

TM = f"{CATALOG}.base_datasets.v_github_teams_members_current"

out("teams.schema", schema(TM))

out("teams.count_demo", count(f"SELECT COUNT(*) FROM {TM} WHERE org_name = '{ORG}'"))

out("teams.sample", rows(f"""
    SELECT org_name, team_name, assignee_login_id
    FROM {TM} WHERE org_name = '{ORG}'
""", 10))

out("teams.distinct_teams", rows(f"""
    SELECT team_name, COUNT(*) AS members
    FROM {TM} WHERE org_name = '{ORG}'
    GROUP BY team_name ORDER BY members DESC
""", 10))

# ── v_itsm_issues_hist (for feature delivery rate / story completion) ────────

IH = f"{CATALOG}.base_datasets.v_itsm_issues_hist"

out("itsm_hist.schema", schema(IH))

# First check all distinct projects so we know what name to filter on
out("itsm_hist.all_distinct_projects", rows(f"""
    SELECT issue_project, itsm_source, COUNT(*) AS n FROM {IH}
    GROUP BY issue_project, itsm_source ORDER BY n DESC
""", 20))

DEMO_PROJECTS = "('ACME', 'Acme Platform', 'acme', 'acme platform')"

out("itsm_hist.count_demo", count(f"""
    SELECT COUNT(*) FROM {IH}
    WHERE lower(issue_project) IN ('acme', 'acme platform')
"""))

out("itsm_hist.distinct_projects_demo", rows(f"""
    SELECT issue_project, COUNT(*) AS n FROM {IH}
    WHERE lower(issue_project) IN ('acme', 'acme platform')
    GROUP BY issue_project ORDER BY n DESC
""", 10))

out("itsm_hist.sprint_sample", rows(f"""
    SELECT issue_project, sprint_name, sprint_state, sprint_complete_date,
           issue_key, issue_type, issue_status, issue_priority
    FROM {IH}
    WHERE lower(issue_project) IN ('acme', 'acme platform')
    AND sprint_name IS NOT NULL
    AND itsm_source = 'jira'
    ORDER BY sprint_complete_date DESC
""", 10))

out("itsm_hist.sprint_state_breakdown", rows(f"""
    SELECT sprint_state, COUNT(*) AS n FROM {IH}
    WHERE lower(issue_project) IN ('acme', 'acme platform')
    AND sprint_name IS NOT NULL
    GROUP BY sprint_state
""", 10))

# ── filter group check (v_filter_group_values_kpi_flattened_unity) ───────────

FV = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"

out("filter_flattened.demo_seed_rows", rows(f"""
    SELECT level_1, level_2, level_3, level_4, id,
           project_url, project_name, team_names, board_ids,
           issue_status, include_issue_types, fix_version
    FROM {FV}
    WHERE created_by = 'seed-data@demo.io'
       OR lower(concat_ws(' ', level_1, level_2, level_3, level_4)) RLIKE 'demo|acme'
    ORDER BY level_1, level_2
""", 30))

out("filter_flattened.project_url_populated_for_seed", rows(f"""
    SELECT id, level_2, level_3, project_url, team_names, board_ids
    FROM {FV}
    WHERE created_by = 'seed-data@demo.io'
    AND project_url IS NOT NULL AND SIZE(project_url) > 0
""", 20))
