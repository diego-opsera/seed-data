# Phase 0 / step 2 — per-table verdict for every DVI source.
#
# For each table the DVI ETL reads, answers: how many rows, which orgs, what
# date coverage, and (where it matters) whether the *values* match the literal
# predicates syncDvi.js filters on. The goal is a verdict of
#   absent / present-but-empty / present-but-wrong-shape / usable
# for each source before any generator is written.
#
# Column shapes mirror visualforge@b8377ed22 exactly — several sources are JSON
# blobs read with get_json_object, not flat columns.
#
# READ-ONLY. No INSERT / UPDATE / DELETE / CREATE.
#
# Run:
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/diag_sources.py").read())

from datetime import date

CATALOG = "playground_prod"

# The ETL default is syncDvi(months = 12); dateMonthsAgo(12) from today.
_today = date.today()
_y, _m = _today.year, _today.month - 12
while _m <= 0:
    _m += 12
    _y -= 1
FROM_DATE = date(_y, _m, min(_today.day, 28)).isoformat()
TO_DATE = _today.isoformat()
CUR_MONTH = _today.strftime("%Y-%m")

# Exact literal sets from the ETL — a value outside these is silently ignored.
DONE_STATUSES = (
    "'done','closed','resolved','completed','complete','released',"
    "'ready for production','ready for release','approved for deploy',"
    "'product deployment','fixed'"
)
FEATURE_TYPES = (
    "'story','feature','new feature','enhancement','epic','user story',"
    "'product backlog item'"
)
DEFECT_PRIORITIES = "'BLOCKER','CRITICAL','HIGHEST','1','1 - CRITICAL'"


def hr(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def q(sql, n=40):
    """Run a query and show it; never raise."""
    try:
        spark.sql(sql).show(n, truncate=False)
    except Exception as e:
        print(f"  QUERY FAILED — {str(e).splitlines()[0][:200]}")


print(f"DVI ETL window replicated: {FROM_DATE} → {TO_DATE}   (current month = {CUR_MONTH})")
print("Velocity + Throughput snapshot tiles read ONLY the latest month with PR data.")

# ─────────────────────────────────────────────────────────────────────────────
hr("1. PR sources — Velocity, Throughput, trend")
print("All three sources are queried and MERGED by sharedToolLoader (dedup on pr_id:repository),")
print("so any of them contributes. Watch the current month specifically.\n")

print("-- source_to_stage.raw_github_pull_requests_rest_api_prs (JSON col `pull_requests`) --")
print("   NOTE: the ETL filters this source on merged_at IS NOT NULL, so it yields MERGED PRs only,")
print("         but buckets them by created_at. A PR created this month must also be MERGED to count.")
q(f"""
  SELECT date_format(to_date(get_json_object(pull_requests, '$.created_at')), 'yyyy-MM') AS created_month,
         COUNT(*) AS prs,
         SUM(CASE WHEN get_json_object(pull_requests, '$.merged_at') IS NOT NULL THEN 1 ELSE 0 END) AS merged,
         COUNT(DISTINCT get_json_object(pull_requests, '$.base.repo.owner.login')) AS owners,
         ROUND(percentile_approx(
           CASE WHEN get_json_object(pull_requests, '$.merged_at') IS NOT NULL
                THEN (unix_timestamp(to_timestamp(get_json_object(pull_requests, '$.merged_at')))
                    - unix_timestamp(to_timestamp(get_json_object(pull_requests, '$.created_at')))) / 3600.0
           END, 0.5), 2) AS median_cycle_hours
  FROM {CATALOG}.source_to_stage.raw_github_pull_requests_rest_api_prs
  WHERE to_date(get_json_object(pull_requests, '$.created_at')) >= '{FROM_DATE}'
  GROUP BY 1 ORDER BY 1
""")

print("-- base_datasets.pull_requests (we already seed this; ETL fallback `base-datasets`) --")
q(f"""
  SELECT org_name,
         date_format(to_date(pr_created_date), 'yyyy-MM') AS created_month,
         COUNT(DISTINCT merge_request_id) AS prs,
         SUM(CASE WHEN pr_merged_datetime IS NOT NULL THEN 1 ELSE 0 END) AS merged,
         ROUND(percentile_approx(
           CASE WHEN pr_merged_datetime IS NOT NULL
                THEN (unix_timestamp(to_timestamp(pr_merged_datetime))
                    - unix_timestamp(to_timestamp(COALESCE(pr_created_datetime, pr_created_date)))) / 3600.0
           END, 0.5), 2) AS median_cycle_hours
  FROM {CATALOG}.base_datasets.pull_requests
  WHERE to_date(pr_created_date) >= '{FROM_DATE}'
  GROUP BY 1, 2 ORDER BY 1, 2
""", 60)

print("-- consumption_layer.commits_prs (we already seed this; ETL `cl-supplement`) --")
print("   NOTE: the CL supplement requires github_commit_email LIKE '%@%' — rows without an")
print("         email are dropped entirely.")
q(f"""
  SELECT org_name,
         date_format(to_date(pr_created_date), 'yyyy-MM') AS created_month,
         COUNT(DISTINCT pr_id) AS prs,
         COUNT(DISTINCT CASE WHEN pr_merged_date IS NOT NULL THEN pr_id END) AS merged,
         COUNT(DISTINCT lower(trim(github_commit_email))) AS distinct_emails,
         SUM(CASE WHEN github_commit_email IS NULL
                    OR lower(trim(github_commit_email)) NOT LIKE '%@%' THEN 1 ELSE 0 END) AS rows_no_email
  FROM {CATALOG}.consumption_layer.commits_prs
  WHERE pr_id IS NOT NULL AND to_date(pr_created_date) >= '{FROM_DATE}'
  GROUP BY 1, 2 ORDER BY 1, 2
""", 60)

# ─────────────────────────────────────────────────────────────────────────────
hr("2. Commit sources — per-developer throughput, individuals[]")

print("-- source_to_stage.raw_github_commits_rest_api (JSON col `commit_details`) --")
q(f"""
  SELECT org_name,
         date_format(to_date(get_json_object(commit_details, '$.commit.author.date')), 'yyyy-MM') AS commit_month,
         COUNT(*) AS commits,
         COUNT(DISTINCT lower(trim(get_json_object(commit_details, '$.commit.author.email')))) AS authors
  FROM {CATALOG}.source_to_stage.raw_github_commits_rest_api
  WHERE to_date(get_json_object(commit_details, '$.commit.author.date')) >= '{FROM_DATE}'
  GROUP BY 1, 2 ORDER BY 1, 2
""", 60)

print("-- base_datasets.commits_rest_api (we already seed this) --")
q(f"""
  SELECT org_name,
         date_format(to_date(commit_date), 'yyyy-MM') AS commit_month,
         COUNT(DISTINCT commit_id) AS commits,
         COUNT(DISTINCT lower(trim(commit_email))) AS authors
  FROM {CATALOG}.base_datasets.commits_rest_api
  WHERE to_date(commit_date) >= '{FROM_DATE}'
  GROUP BY 1, 2 ORDER BY 1, 2
""", 60)

# ─────────────────────────────────────────────────────────────────────────────
hr("3. ITSM — Quality (defect leakage) + Impact (features shipped)")
print("Both read base_datasets.v_itsm_issues_hist. NEITHER query filters by org.\n")

print("-- coverage by itsm_source --")
q(f"""
  SELECT itsm_source,
         COUNT(*) AS rows,
         COUNT(DISTINCT issue_key) AS issues,
         MIN(to_date(issue_created_date)) AS min_created,
         MAX(to_date(issue_created_date)) AS max_created,
         COUNT(DISTINCT lower(trim(assignee_email))) AS distinct_assignee_emails
  FROM {CATALOG}.base_datasets.v_itsm_issues_hist
  GROUP BY itsm_source ORDER BY rows DESC
""")

print("-- distinct issue_priority values (Quality counts ONLY the listed set) --")
print(f"   ETL requires: UPPER(issue_priority) IN ({DEFECT_PRIORITIES})")
q(f"""
  SELECT upper(trim(issue_priority)) AS issue_priority_upper,
         COUNT(*) AS rows,
         CASE WHEN upper(trim(issue_priority)) IN ({DEFECT_PRIORITIES})
              THEN 'COUNTS as high-priority' ELSE 'ignored by Quality' END AS verdict
  FROM {CATALOG}.base_datasets.v_itsm_issues_hist
  GROUP BY 1 ORDER BY rows DESC
""")

print("-- distinct issue_status values (Impact counts ONLY the completion set) --")
q(f"""
  SELECT lower(trim(issue_status)) AS issue_status_lower,
         COUNT(*) AS rows,
         CASE WHEN lower(trim(issue_status)) IN ({DONE_STATUSES})
              THEN 'COUNTS as completed' ELSE 'ignored by Impact' END AS verdict
  FROM {CATALOG}.base_datasets.v_itsm_issues_hist
  GROUP BY 1 ORDER BY rows DESC
""")

print("-- distinct issue_type values (Impact feature set / Quality defect set) --")
q(f"""
  SELECT lower(trim(issue_type)) AS issue_type_lower,
         COUNT(*) AS rows,
         CASE WHEN lower(trim(issue_type)) IN ({FEATURE_TYPES}) THEN 'feature (Impact)'
              WHEN lower(trim(issue_type)) IN ('bug','defect')  THEN 'defect (Quality)'
              ELSE 'neither' END AS verdict
  FROM {CATALOG}.base_datasets.v_itsm_issues_hist
  GROUP BY 1 ORDER BY rows DESC
""")

print("-- story_points + resolution-date completeness (Impact needs resolution_date NOT NULL) --")
print("   NOTE: story_points is a STRING column holding values like '2.0'; always try_cast it.")
q(f"""
  SELECT COUNT(*) AS rows,
         SUM(CASE WHEN issue_resolution_date IS NULL THEN 1 ELSE 0 END) AS null_resolution_date,
         SUM(CASE WHEN issue_updated_date   IS NULL THEN 1 ELSE 0 END) AS null_updated_date,
         SUM(CASE WHEN try_cast(story_points AS DOUBLE) IS NULL
                    OR try_cast(story_points AS DOUBLE) = 0 THEN 1 ELSE 0 END) AS no_story_points,
         SUM(CASE WHEN assignee_email IS NULL OR trim(assignee_email) = '' THEN 1 ELSE 0 END) AS no_assignee_email
  FROM {CATALOG}.base_datasets.v_itsm_issues_hist
  WHERE to_date(issue_created_date) >= '{FROM_DATE}'
""")

# ─────────────────────────────────────────────────────────────────────────────
hr("4. Security — three-tier fallback (tier 1 wins if it has ANY rows)")

print("-- TIER 1: base_datasets.asp_sonar_issues — NO date filter, NO org filter, whole table --")
print("   passRate = resolved_vulnerabilities / total_vulnerabilities * 100")
q(f"""
  SELECT upper(trim(type)) AS type_upper,
         upper(trim(status)) AS status_upper,
         COUNT(*) AS rows
  FROM {CATALOG}.base_datasets.asp_sonar_issues
  GROUP BY 1, 2 ORDER BY rows DESC
""")
print("   → tier-1 verdict (total_vulnerabilities > 0 means tiers 2 and 3 never run):")
q(f"""
  SELECT COUNT(*) AS total_issues,
         SUM(CASE WHEN upper(type) = 'VULNERABILITY' THEN 1 ELSE 0 END) AS total_vulnerabilities,
         SUM(CASE WHEN upper(type) = 'VULNERABILITY'
                   AND upper(status) IN ('RESOLVED','CLOSED') THEN 1 ELSE 0 END) AS resolved_vulnerabilities
  FROM {CATALOG}.base_datasets.asp_sonar_issues
""")

print("-- TIER 2: source_to_stage.raw_sonar_metric_split_data_branchwise (also the monthly series) --")
q(f"""
  SELECT org_name,
         date_format(to_date(last_analysis_date), 'yyyy-MM') AS analysis_month,
         COUNT(*) AS rows,
         SUM(CASE WHEN quality_gate_status = 'OK' THEN 1 ELSE 0 END) AS gates_ok,
         COUNT(DISTINCT project_name) AS projects
  FROM {CATALOG}.source_to_stage.raw_sonar_metric_split_data_branchwise
  WHERE last_analysis_date IS NOT NULL AND to_date(last_analysis_date) >= '{FROM_DATE}'
  GROUP BY 1, 2 ORDER BY 1, 2
""", 60)
print("   NOTE: the gate test is a case-sensitive equality on 'OK' — 'ok'/'PASSED' do not count.")
q(f"""
  SELECT quality_gate_status, COUNT(*) AS rows
  FROM {CATALOG}.source_to_stage.raw_sonar_metric_split_data_branchwise
  GROUP BY 1 ORDER BY rows DESC
""")

print("-- per-developer Sonar: source_to_stage.raw_sonar_type_data_branchwise --")
q(f"""
  SELECT COUNT(*) AS rows,
         COUNT(DISTINCT lower(trim(author))) AS distinct_authors
  FROM {CATALOG}.source_to_stage.raw_sonar_type_data_branchwise
""")

print("-- TIER 3: source_to_stage.github_actions_runs_rest_api (JSON col `message`) --")
print("   security workflows = name LIKE any of %security% %scan% %codeql% %sast% %dast%")
print("                        %vulnerability% %snyk% %trivy% %dependabot% %quality% %lint% %test%")
q(f"""
  SELECT COUNT(*) AS total_runs,
         SUM(CASE WHEN lower(get_json_object(message, '$.conclusion')) = 'success' THEN 1 ELSE 0 END) AS success_runs,
         COUNT(DISTINCT get_json_object(message, '$.name')) AS distinct_workflow_names,
         MIN(to_date(record_insert_datetime)) AS min_insert,
         MAX(to_date(record_insert_datetime)) AS max_insert
  FROM {CATALOG}.source_to_stage.github_actions_runs_rest_api
  WHERE record_insert_datetime >= '{FROM_DATE}'
    AND get_json_object(message, '$.conclusion') IS NOT NULL
""")

# ─────────────────────────────────────────────────────────────────────────────
hr("5. Deploys — Impact trend (pipeline source preferred over GHA)")

print("-- source_to_stage.raw_mongo_pipelineactivities (preferred) --")
q(f"""
  SELECT date_format(to_date(createdAt), 'yyyy-MM') AS deploy_month,
         COUNT(DISTINCT _id) AS deploys
  FROM {CATALOG}.source_to_stage.raw_mongo_pipelineactivities
  WHERE createdAt >= '{FROM_DATE}'
  GROUP BY 1 ORDER BY 1
""", 60)

# ─────────────────────────────────────────────────────────────────────────────
hr("6. master_data.date_dim — join spine for EVERY time series")
print("If this does not span the seed window, charts render EMPTY with no error.")
q(f"""
  SELECT MIN(calendar_date) AS min_date,
         MAX(calendar_date) AS max_date,
         COUNT(*) AS rows,
         SUM(CASE WHEN week_start_date  IS NULL THEN 1 ELSE 0 END) AS null_week_start,
         SUM(CASE WHEN month_start_date IS NULL THEN 1 ELSE 0 END) AS null_month_start
  FROM {CATALOG}.master_data.date_dim
""")
print(f"   required coverage: {FROM_DATE} → {TO_DATE} inclusive")

# ─────────────────────────────────────────────────────────────────────────────
hr("7. Team labels — Team Split / individuals[].team")
print("Without these, every developer falls back to team = 'Engineering'.")
print("See BUGS.md section 2: this view's global-MAX pattern can hide other orgs after an insert.")
q(f"""
  SELECT org_name, COUNT(*) AS rows, COUNT(DISTINCT team_name) AS teams
  FROM {CATALOG}.base_datasets.v_github_teams_members_current
  GROUP BY org_name ORDER BY rows DESC
""")

# ─────────────────────────────────────────────────────────────────────────────
hr("8. Existing org identifiers — confirm demo-acme-vf is unused")
print("Phase 1 introduces demo-acme-vf. It must not collide with anything already seeded.")
for tbl, col in [
    ("base_datasets.pull_requests", "org_name"),
    ("base_datasets.commits_rest_api", "org_name"),
    ("consumption_layer.commits_prs", "org_name"),
]:
    print(f"\n-- {tbl}.{col} --")
    q(f"SELECT {col}, COUNT(*) AS rows FROM {CATALOG}.{tbl} GROUP BY 1 ORDER BY rows DESC", 30)

hr("Done — next: diag_dimensions.py")
print("diag_dimensions.py replays the ETL's own dimension resolution to predict each tile value.")
