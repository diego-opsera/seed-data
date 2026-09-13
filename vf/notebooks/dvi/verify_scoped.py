# Verify the demo-acme-vf seed from the Databricks side alone.
#
# Runs the DVI ETL's own predicates against our rows only. If the dimensions
# resolve here, the data satisfies the ETL contract — anything still dark in the
# UI after that is the ETL run or the mapping-group scope, not the seed.
#
# The single most important section is 4: the cross-source identity overlap.
# VisualForge joins commits / PRs / Jira / Sonar on LOWER(TRIM(email)) and drops
# non-matching rows SILENTLY, so a mismatch costs a developer in individuals[]
# with no error anywhere. That is already true of demo-acme-direct, whose 37,598
# commits carry zero emails (BUGS.md #12).
#
# READ-ONLY.
#
# Run:
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/verify_scoped.py").read())

import json
from datetime import date

CATALOG = "playground_prod"
ORG = "demo-acme-vf"
SEEDED_BY = "seed-data-vf"

_today = date.today()
_y, _m = _today.year, _today.month - 12
while _m <= 0:
    _m += 12
    _y -= 1
FROM_DATE = date(_y, _m, min(_today.day, 28)).isoformat()
TO_DATE = _today.isoformat()
CUR_MONTH = _today.strftime("%Y-%m")

DONE_STATUSES = ("'done','closed','resolved','completed','complete','released',"
                 "'ready for production','ready for release','approved for deploy',"
                 "'product deployment','fixed'")
FEATURE_TYPES = ("'story','feature','new feature','enhancement','epic','user story',"
                 "'product backlog item'")
DEFECT_PRIORITIES = "'BLOCKER','CRITICAL','HIGHEST','1','1 - CRITICAL'"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=40):
    try:
        return [r.asDict() for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return {"error": str(e).splitlines()[0][:250]}


print(f"## verify {ORG} — ETL window {FROM_DATE} → {TO_DATE} (current month {CUR_MONTH})")

# ── 1. Velocity + Throughput ────────────────────────────────────────────────
# Bucketed by created_at, which is what syncDvi does. Only MERGED PRs count.
out("1. prs_by_created_month", rows(f"""
    SELECT date_format(to_date(get_json_object(pull_requests, '$.created_at')), 'yyyy-MM') AS created_month,
           COUNT(*) AS prs,
           SUM(CASE WHEN get_json_object(pull_requests, '$.merged_at') IS NOT NULL THEN 1 ELSE 0 END) AS merged,
           ROUND(percentile_approx(
             CASE WHEN get_json_object(pull_requests, '$.merged_at') IS NOT NULL THEN
               (unix_timestamp(to_timestamp(get_json_object(pull_requests, '$.merged_at')))
              - unix_timestamp(to_timestamp(get_json_object(pull_requests, '$.created_at')))) / 3600.0
             END, 0.5), 2) AS median_cycle_hours
    FROM {CATALOG}.source_to_stage.raw_github_pull_requests_rest_api_prs
    WHERE owner = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
    GROUP BY 1 ORDER BY 1
""", 24))

# The insert gate: TO_DATE(record_insert_datetime) <= ETL end date.
out("1b. prs_failing_insert_gate", rows(f"""
    SELECT COUNT(*) AS rows_gated_out
    FROM {CATALOG}.source_to_stage.raw_github_pull_requests_rest_api_prs
    WHERE owner = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
      AND to_date(record_insert_datetime) > '{TO_DATE}'
      AND to_date(get_json_object(pull_requests, '$.merged_at')) <= '{TO_DATE}'
""", 1))

# ── 2. Commits ──────────────────────────────────────────────────────────────
out("2. commits_by_month", rows(f"""
    SELECT date_format(to_date(get_json_object(commit_details, '$.commit.author.date')), 'yyyy-MM') AS commit_month,
           COUNT(*) AS commits,
           COUNT(DISTINCT lower(trim(get_json_object(commit_details, '$.commit.author.email')))) AS authors,
           SUM(CASE WHEN get_json_object(commit_details, '$.commit.author.email') IS NULL THEN 1 ELSE 0 END) AS null_email
    FROM {CATALOG}.source_to_stage.raw_github_commits_rest_api
    WHERE org_name = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
    GROUP BY 1 ORDER BY 1
""", 24))

# ── 3. Quality + Impact ─────────────────────────────────────────────────────
out("3a. quality_defect_leakage", rows(f"""
    WITH issues AS (
      SELECT issue_key, issue_type, issue_priority,
             ROW_NUMBER() OVER (PARTITION BY issue_key ORDER BY issue_updated_date DESC) AS rn
      FROM {CATALOG}.base_datasets.v_itsm_issues_hist
      WHERE customer_id = '{ORG}'
        AND issue_created_date IS NOT NULL
        AND to_date(issue_created_date) BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
    )
    SELECT COUNT(DISTINCT issue_key) AS total_issues,
           COUNT(DISTINCT CASE WHEN lower(trim(issue_type)) IN ('bug','defect')
                                AND upper(trim(issue_priority)) IN ({DEFECT_PRIORITIES})
                               THEN issue_key END) AS high_priority_defects,
           ROUND(100.0 * COUNT(DISTINCT CASE WHEN lower(trim(issue_type)) IN ('bug','defect')
                                AND upper(trim(issue_priority)) IN ({DEFECT_PRIORITIES})
                               THEN issue_key END) / COUNT(DISTINCT issue_key), 2) AS leakage_pct
    FROM issues WHERE rn = 1
""", 1))

out("3b. impact_resolved_features_by_month", rows(f"""
    WITH resolved AS (
      SELECT issue_key, issue_type, issue_updated_date,
             ROW_NUMBER() OVER (PARTITION BY issue_key
                                ORDER BY issue_updated_date DESC, issue_resolution_date DESC) AS rn
      FROM {CATALOG}.base_datasets.v_itsm_issues_hist
      WHERE customer_id = '{ORG}'
        AND issue_resolution_date IS NOT NULL
        AND issue_updated_date BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
        AND lower(trim(issue_status)) IN ({DONE_STATUSES})
    )
    SELECT date_format(issue_updated_date, 'yyyy-MM') AS month,
           COUNT(DISTINCT CASE WHEN lower(trim(issue_type)) IN ({FEATURE_TYPES})
                               THEN issue_key END) AS resolved_features
    FROM resolved WHERE rn = 1 GROUP BY 1 ORDER BY 1
""", 24))

out("3c. itsm_priority_and_status_check", rows(f"""
    SELECT upper(trim(issue_priority)) AS priority,
           lower(trim(issue_type)) AS issue_type,
           COUNT(*) AS n,
           SUM(CASE WHEN issue_resolution_date IS NULL THEN 1 ELSE 0 END) AS unresolved
    FROM {CATALOG}.base_datasets.v_itsm_issues_hist
    WHERE customer_id = '{ORG}'
    GROUP BY 1, 2 ORDER BY n DESC
""", 20))

# ── 4. THE identity join — the whole batch hinges on this ───────────────────
out("4. identity_overlap", rows(f"""
    WITH commit_emails AS (
      SELECT DISTINCT lower(trim(get_json_object(commit_details, '$.commit.author.email'))) AS email
      FROM {CATALOG}.source_to_stage.raw_github_commits_rest_api
      WHERE org_name = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
    ),
    itsm_emails AS (
      SELECT DISTINCT lower(trim(assignee_email)) AS email
      FROM {CATALOG}.base_datasets.v_itsm_issues_hist
      WHERE customer_id = '{ORG}'
    ),
    pr_logins AS (
      SELECT DISTINCT lower(trim(get_json_object(pull_requests, '$.user.login'))) AS login
      FROM {CATALOG}.source_to_stage.raw_github_pull_requests_rest_api_prs
      WHERE owner = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
    )
    SELECT (SELECT COUNT(*) FROM commit_emails) AS commit_emails,
           (SELECT COUNT(*) FROM itsm_emails)   AS itsm_emails,
           (SELECT COUNT(*) FROM pr_logins)     AS pr_logins,
           (SELECT COUNT(*) FROM commit_emails c JOIN itsm_emails i ON c.email = i.email) AS commit_itsm_overlap,
           (SELECT COUNT(*) FROM commit_emails c JOIN pr_logins p
              ON split_part(c.email, '@', 1) = p.login) AS commit_pr_login_overlap
""", 1))
print("  commit_emails == itsm_emails == commit_itsm_overlap means every developer")
print("  resolves identically across SCM and Jira. Anything lower is a silent drop.")

# ── 5. Security — per-repo GHA pass rate (the route around the unscoped tier 1) ─
out("5. gha_pass_rate_by_repo", rows(f"""
    WITH parsed AS (
      SELECT repo_url,
             get_json_object(message, '$.name') AS workflow_name,
             lower(get_json_object(message, '$.conclusion')) AS conclusion
      FROM {CATALOG}.source_to_stage.github_actions_runs_rest_api
      WHERE owner = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
        AND record_insert_datetime BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
        AND get_json_object(message, '$.conclusion') IS NOT NULL
    )
    SELECT repo_url,
           COUNT(*) AS total_runs,
           SUM(CASE WHEN conclusion = 'success' THEN 1 ELSE 0 END) AS success_runs,
           ROUND(100.0 * SUM(CASE WHEN conclusion = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pass_rate,
           SUM(CASE WHEN lower(workflow_name) LIKE '%security%' OR lower(workflow_name) LIKE '%scan%'
                      OR lower(workflow_name) LIKE '%codeql%'   OR lower(workflow_name) LIKE '%lint%'
                      OR lower(workflow_name) LIKE '%test%'     OR lower(workflow_name) LIKE '%quality%'
                    THEN 1 ELSE 0 END) AS security_gate_runs
    FROM parsed GROUP BY 1 ORDER BY total_runs DESC
""", 12))

out("6. rows_seeded_per_table", rows(f"""
    SELECT 'raw_github_commits_rest_api' AS tbl, COUNT(*) AS n
      FROM {CATALOG}.source_to_stage.raw_github_commits_rest_api
      WHERE org_name = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
    UNION ALL SELECT 'raw_github_pull_requests_rest_api_prs', COUNT(*)
      FROM {CATALOG}.source_to_stage.raw_github_pull_requests_rest_api_prs
      WHERE owner = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
    UNION ALL SELECT 'github_actions_runs_rest_api', COUNT(*)
      FROM {CATALOG}.source_to_stage.github_actions_runs_rest_api
      WHERE owner = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
    UNION ALL SELECT 'mt_itsm_issues_hist', COUNT(*)
      FROM {CATALOG}.transform_stage.mt_itsm_issues_hist
      WHERE customer_id = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
    UNION ALL SELECT 'mt_itsm_issues_current', COUNT(*)
      FROM {CATALOG}.transform_stage.mt_itsm_issues_current
      WHERE customer_id = '{ORG}' AND record_inserted_by = '{SEEDED_BY}'
""", 10))

print("\n## done")
print("If section 4 shows full overlap and sections 1-3 return non-zero for the")
print("current month, the Databricks side is correct. The dashboard then depends on")
print("the scheduled ETL run and the mapping-group scope.")
