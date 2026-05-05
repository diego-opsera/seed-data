# Create the denormalized fact table that powers the Issue Stream / Flow View
# feature in vnxt-insights-api (src/queries/value-stream/*.sql).
#
# The schema `playground_prod.user_working` already exists (confirmed by
# diag_1.py, has 4 unrelated tables). The table itself does not exist anywhere
# in playground_prod (also confirmed by diag_1.py's information_schema scan).
#
# Column types are chosen to match how the API queries reference each column:
#   - String literals like 'true'/'false'/'Y'/'open'/'merged'/'failure'    → STRING
#   - UNIX_TIMESTAMP(...) inputs                                           → TIMESTAMP
#   - CAST(... AS BIGINT) inputs (line counts, story points, copilot ints) → BIGINT
#   - TRY_CAST(... AS INT) inputs (copilot_total_interactions)             → BIGINT
#                                                                            (TRY_CAST handles bigint→int)
#
# Known API-side bug (will be added to BUGS.md): copilot_commit_flag is
# referenced as `= true` (bool) in issue-stream-list.sql:20 but as `= 'Y'`
# (string) in flow-dashboard-commit-metrics.sql and flow-dashboard-aiassist.sql.
# We store STRING 'Y' to satisfy 2 of 3 queries.
#
# Run via:
#   exec(open("/tmp/seed-data/notebooks/value_stream/create_table.py").read())
#
# Idempotent: CREATE TABLE IF NOT EXISTS — safe to re-run.

CATALOG = "playground_prod"
SCHEMA  = "user_working"
TABLE   = "offerings_jira_pipeline_details"
FQN     = f"{CATALOG}.{SCHEMA}.{TABLE}"

_TBLPROPS = """
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors'   = 'true',
  'delta.feature.appendOnly'      = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants'      = 'supported',
  'delta.minReaderVersion'        = '3',
  'delta.minWriterVersion'        = '7')
"""

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN} (
  -- ── Hierarchy / scope ──────────────────────────────────────────────────────
  sbg                          STRING,
  gbe                          STRING,
  offering                     STRING,
  jira_sprint                  STRING,
  org_name                     STRING,
  project_name                 STRING,
  jira_project                 STRING,

  -- ── Jira issue ─────────────────────────────────────────────────────────────
  ticket_key                   STRING,
  jira_issue_type              STRING,
  jira_summary                 STRING,
  jira_priority                STRING,
  jira_severity                STRING,
  jira_status                  STRING,
  jira_resolution              STRING,
  jira_assignee                STRING,
  jira_story_points            BIGINT,
  jira_issue_link              STRING,

  -- ── Commit ─────────────────────────────────────────────────────────────────
  commit_id                    STRING,
  commit_date                  DATE,
  commit_timestamp             TIMESTAMP,
  commit_author                STRING,
  commit_github_login          STRING,
  commit_title                 STRING,
  copilot_commit_flag          STRING,    -- 'Y' or NULL (see header note)
  commit_lines_added           BIGINT,
  commit_lines_removed         BIGINT,
  commit_lines_modified        BIGINT,
  is_merge_commit              STRING,    -- 'true' / 'false'
  is_bot_commit                STRING,    -- 'true' / 'false'

  -- ── Pull Request ───────────────────────────────────────────────────────────
  pr_id                        STRING,
  pr_title                     STRING,
  pr_url                       STRING,
  pr_state                     STRING,    -- 'open' / 'merged' / 'closed'
  pr_merged                    STRING,    -- 'true' / 'false'
  pr_user_name                 STRING,
  pr_created_datetime          TIMESTAMP,
  pr_merged_datetime           TIMESTAMP,
  pr_source_branch             STRING,
  pr_target_branch             STRING,

  -- ── Pipeline run + step ────────────────────────────────────────────────────
  pipeline_id                  STRING,
  pipeline_name                STRING,
  pipeline_status              STRING,
  pipeline_event_type          STRING,
  pipeline_started_at          TIMESTAMP,
  pipeline_finished_at         TIMESTAMP,
  pipeline_step_name           STRING,
  pipeline_step_type           STRING,
  pipeline_step_status         STRING,
  pipeline_step_conclusion     STRING,    -- 'success' / 'failure'
  pipeline_branch              STRING,
  pipeline_commit_sha          STRING,

  -- ── Copilot author-level rollup (per-row, denormalized from author stats) ──
  copilot_active_days          BIGINT,
  copilot_total_interactions   BIGINT,
  copilot_code_generations     BIGINT,
  copilot_code_acceptances     BIGINT,
  copilot_acceptance_rate_pct  DOUBLE,
  copilot_loc_suggested        BIGINT,
  copilot_loc_added            BIGINT,
  copilot_used_chat            STRING,    -- 'true' / 'false'
  copilot_used_agent           STRING     -- 'true' / 'false'
) {_TBLPROPS}
""")
print(f"Created (or kept): {FQN}")

# ── repo_pipeline_details ────────────────────────────────────────────────────
# Bridge table for the Pipeline Failures feature. Each row mirrors a failed
# pipeline-step run from base_datasets.pipeline_activities (joined via
# pipeline_id + project_name) so the page can display log content from the
# logs table alongside the issue context.

RPD_FQN = f"{CATALOG}.{SCHEMA}.repo_pipeline_details"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RPD_FQN} (
  pipeline_id              STRING,
  org_name                 STRING,
  project_name             STRING,
  pipeline_name            STRING,
  pipeline_status          STRING,
  pipeline_step_name       STRING,
  pipeline_step_conclusion STRING,    -- 'success' / 'failure'
  pipeline_started_at      TIMESTAMP,
  pipeline_finished_at     TIMESTAMP,
  pipeline_branch          STRING,
  pipeline_commit_sha      STRING,
  ticket_key               STRING,
  record_inserted_by       STRING     -- scope tag for safe deletion
) {_TBLPROPS}
""")
print(f"Created (or kept): {RPD_FQN}")

# ── github_offering_workflow_job_logs ────────────────────────────────────────
# Stores raw build/test/deploy step output. The Pipeline Failures SQL joins
# CAST(pa.step_id AS STRING) = CAST(logs.job AS STRING).

LOGS_FQN = f"{CATALOG}.{SCHEMA}.github_offering_workflow_job_logs"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOGS_FQN} (
  job                STRING,
  logs               STRING,
  record_inserted_by STRING            -- scope tag for safe deletion
) {_TBLPROPS}
""")
print(f"Created (or kept): {LOGS_FQN}")

# ── Verify ───────────────────────────────────────────────────────────────────
for fqn in [FQN, RPD_FQN, LOGS_FQN]:
    n = spark.sql(f"SELECT COUNT(*) FROM {fqn}").collect()[0][0]
    cols = spark.sql(f"DESCRIBE {fqn}").collect()
    n_cols = len([r for r in cols if r["col_name"] and not r["col_name"].startswith("#")])
    print(f"  {fqn.split('.')[-1]}: {n} rows, {n_cols} columns")
