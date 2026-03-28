TARGET_CATALOG = "playground_prod"
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
