# Diagnostic round 2: figure out why /issue-stream returns 500 for ACME-1001
#
# Two hypotheses:
#  A. insert.py never ran — table is empty, SQL returns no rows but somehow 500s
#  B. UNION ALL in issue-stream.sql mismatches types (commit_date is DATE while
#     pr_created_datetime and pipeline_started_at are TIMESTAMP — all aliased
#     to `timestamp` in the same UNION position).
#
# Read-only. Run via:
#   exec(open("/tmp/seed-data/notebooks/value_stream/diag_2.py").read())

import json

CATALOG = "playground_prod"
FQN     = f"{CATALOG}.user_working.offerings_jira_pipeline_details"

def sql(q):
    return spark.sql(q)

def rows(q, limit=20):
    return [r.asDict() for r in sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

def safe(label, fn):
    try:
        out(label, fn())
    except Exception as e:
        # Trim long Spark stack traces
        msg = str(e).split("\n")[0]
        out(label, {"error": msg})


# ── 1. Total rows + per-org counts ──────────────────────────────────────────

safe("total_rows", lambda: sql(f"SELECT COUNT(*) AS n FROM {FQN}").collect()[0]["n"])

safe("rows_by_org", lambda: rows(f"""
    SELECT org_name, COUNT(*) AS n
    FROM {FQN}
    GROUP BY org_name
    ORDER BY n DESC
"""))

# ── 2. Specifically: does ACME-1001 exist? ──────────────────────────────────

safe("acme_1001_count", lambda: sql(f"""
    SELECT COUNT(*) AS n
    FROM {FQN}
    WHERE ticket_key = 'ACME-1001'
""").collect()[0]["n"])

safe("acme_1001_breakdown", lambda: rows(f"""
    SELECT
      COUNT(*) AS total_rows,
      COUNT(DISTINCT commit_id)   AS distinct_commits,
      COUNT(DISTINCT pr_id)       AS distinct_prs,
      COUNT(DISTINCT pipeline_id) AS distinct_pipelines
    FROM {FQN}
    WHERE ticket_key = 'ACME-1001'
"""))

# ── 3. Run the exact issue-stream.sql against ACME-1001 ─────────────────────
# This replicates the API's query verbatim. If it succeeds, the SQL is fine
# and the 500 is somewhere else. If it errors, we get the exact Spark message.

ISSUE_STREAM_SQL = f"""
WITH base AS (
  SELECT
    commit_id, commit_date, commit_author, commit_title, copilot_commit_flag,
    commit_lines_added, commit_lines_removed, commit_lines_modified,
    pr_id, pr_created_datetime, pr_user_name, pr_title, pr_state,
    pipeline_id, pipeline_started_at, pipeline_step_name,
    pipeline_step_conclusion, pipeline_step_type,
    org_name, project_name
  FROM {FQN}
  WHERE ticket_key = 'ACME-1001'
    AND (commit_id IS NOT NULL OR pr_id IS NOT NULL
         OR (pipeline_id IS NOT NULL AND pipeline_step_name IS NOT NULL))
),
issue_commits AS (
  SELECT
    'commit' as data_type, commit_id as id, commit_date as timestamp,
    commit_author as author, commit_title as title, copilot_commit_flag,
    commit_lines_added as lines_added, commit_lines_removed as lines_removed,
    commit_lines_modified as lines_modified, org_name, project_name,
    NULL as step_conclusion, NULL as step_type
  FROM base
  WHERE commit_id IS NOT NULL
),
issue_prs AS (
  SELECT
    'pr' as data_type, pr_id as id, pr_created_datetime as timestamp,
    pr_user_name as author, pr_title as title, NULL as copilot_commit_flag,
    NULL as lines_added, NULL as lines_removed, NULL as lines_modified,
    org_name, project_name, NULL as step_conclusion, pr_state as step_type
  FROM base
  WHERE pr_id IS NOT NULL
),
issue_pipelines AS (
  SELECT
    pipeline_step_type as data_type, pipeline_id as id,
    pipeline_started_at as timestamp, NULL as author,
    pipeline_step_name as title, NULL as copilot_commit_flag,
    NULL as lines_added, NULL as lines_removed, NULL as lines_modified,
    org_name, project_name, pipeline_step_conclusion as step_conclusion,
    pipeline_step_type as step_type
  FROM base
  WHERE pipeline_id IS NOT NULL AND pipeline_step_name IS NOT NULL
)
SELECT * FROM issue_commits
UNION ALL SELECT * FROM issue_prs
UNION ALL SELECT * FROM issue_pipelines
ORDER BY timestamp DESC
"""

safe("issue_stream_query.run", lambda: sql(ISSUE_STREAM_SQL).count())
safe("issue_stream_query.sample_5", lambda: rows(ISSUE_STREAM_SQL, 5))

# ── 4. Also re-run the issue-stream-list query (the original broken panel) ──

ISSUE_LIST_SQL = f"""
SELECT
  ticket_key as issue_key,
  MAX(jira_sprint) as sprint,
  COUNT(*) as flow_refs,
  COUNT(DISTINCT CASE WHEN commit_id IS NOT NULL THEN commit_id END) as commit_count,
  COUNT(DISTINCT CASE WHEN pr_id IS NOT NULL THEN pr_id END) as pr_count
FROM {FQN}
WHERE ticket_key IS NOT NULL AND ticket_key != ''
GROUP BY ticket_key
ORDER BY flow_refs DESC, ticket_key
LIMIT 5
"""
safe("issue_list_query.top_5", lambda: rows(ISSUE_LIST_SQL))
