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
  enterprise_id INT,
  file_extension ARRAY<STRUCT<file_extension: STRING, lines: STRUCT<additions: INT, deletions: INT>>>)
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

"code_scan_alert": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.base_datasets.code_scan_alert (
  number BIGINT,
  repository_html_url STRING,
  repository_name STRING,
  created_at TIMESTAMP,
  rule_severity STRING,
  html_url STRING,
  organization STRING,
  state STRING,
  fixed_at TIMESTAMP,
  dismissed_at TIMESTAMP,
  teams ARRAY<STRING>)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

"secret_scan_alert": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.base_datasets.secret_scan_alert (
  number BIGINT,
  repository_html_url STRING,
  repository_name STRING,
  severity STRING,
  html_url STRING,
  organization STRING,
  created_at TIMESTAMP,
  state STRING,
  resolved_at TIMESTAMP,
  teams ARRAY<STRING>)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

"file_extensions": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.master_data.file_extensions (
  code_file_extension STRING,
  code_language STRING)
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

# Add file_extension to commits_rest_api if it was created before this column existed
try:
    spark.sql(f"""
        ALTER TABLE {TARGET_CATALOG}.{TARGET_SCHEMA}.commits_rest_api
        ADD COLUMNS (file_extension ARRAY<STRUCT<file_extension: STRING, lines: STRUCT<additions: INT, deletions: INT>>>)
    """)
    print("Added file_extension column to commits_rest_api")
except Exception:
    pass  # Column already exists

print("\nVerifying...")
verify_tables = {
    "base_datasets": ["trf_github_copilot_direct_data", "commits_rest_api", "pull_requests",
                      "code_scan_alert", "secret_scan_alert"],
    "master_data":   ["file_extensions"],
}
for schema, tables in verify_tables.items():
    for name in tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_CATALOG}.{schema}.{name}").collect()[0][0]
            print(f"  {schema}.{name}: {count} rows")
        except Exception as e:
            print(f"  {schema}.{name}: ERROR — {e}")
