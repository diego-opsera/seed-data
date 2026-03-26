TARGET_CATALOG = "opsera_test"  # change to your playground catalog
TARGET_SCHEMA  = "base_datasets"

ddls = {

"trf_github_copilot_direct_data": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.trf_github_copilot_direct_data (
  date DATE,
  user_login STRING,
  team_name STRING,
  application STRING,
  org_name STRING,
  enterprise_id INT)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

"v_github_copilot_seats_usage_user_level": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.v_github_copilot_seats_usage_user_level (
  record_insert_datetime TIMESTAMP,
  cleansed_assignee_login STRING,
  copilot_usage_date DATE,
  copilot_usage_datetime TIMESTAMP,
  org_name STRING,
  org_assignee_login STRING,
  enterprise_id INT)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

"v_github_copilot_metrics_ide_org_level": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.v_github_copilot_metrics_ide_org_level (
  copilot_usage_date DATE,
  org_name STRING,
  enterprise_id INT,
  ide_code_completion_editor_language STRING,
  ide_code_completion_editor_name STRING,
  ide_code_completion_code_suggestions BIGINT,
  ide_code_completion_code_acceptances BIGINT,
  ide_code_completion_code_lines_suggested BIGINT,
  ide_code_completion_code_lines_accepted BIGINT,
  ide_code_completion_code_lines_suggested_to_add BIGINT,
  ide_code_completion_code_lines_suggested_to_delete BIGINT,
  ide_code_completion_code_lines_accepted_to_add BIGINT,
  ide_code_completion_code_lines_accepted_to_delete BIGINT,
  ide_code_completion_engaged_users BIGINT,
  ide_chat_engaged_users BIGINT,
  agent_engaged_users BIGINT,
  total_active_users BIGINT,
  total_engaged_users BIGINT,
  ide_chat_model_name STRING,
  ide_chat_editor_name STRING,
  ide_chat_editor_chats BIGINT,
  total_interactions_count BIGINT,
  agent_lines_accepted_to_add BIGINT,
  agent_lines_accepted_to_delete BIGINT)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

"commits_rest_api": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.commits_rest_api (
  commit_id STRING,
  commit_date DATE,
  commit_timestamp TIMESTAMP,
  org_name STRING,
  project_name STRING,
  project_url STRING,
  cleansed_user_name STRING,
  cleansed_commit_author STRING,
  user_id INT,
  copilot_commit_flag STRING,
  lines_added INT,
  lines_removed INT,
  before_sha STRING,
  enterprise_id INT)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

# Stub — referenced by commits_rest_api queries via left anti join.
# Left empty; the join is a no-op when :enableCommitLOC = false.
"pull_requests": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.pull_requests (
  merge_request_id STRING,
  pr_user_name STRING,
  org_name STRING,
  lines_added INT,
  lines_removed INT,
  pr_created_datetime TIMESTAMP,
  pr_commits ARRAY<STRUCT<sha: STRING>>,
  board_info ARRAY<STRUCT<board_id: STRING, board_name: STRING>>,
  enterprise_id INT)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",
}

for name, ddl in ddls.items():
    print(f"Creating {name}...", end=" ")
    spark.sql(ddl)
    print("done")

print("\nVerifying...")
for name in ddls:
    count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.{name}").collect()[0][0]
    print(f"  {name}: {count} rows")
