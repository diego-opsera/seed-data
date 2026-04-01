TARGET_CATALOG = "playground_prod"
TARGET_SCHEMA  = "consumption_layer"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Schema {TARGET_CATALOG}.{TARGET_SCHEMA} ready.")

ddl = f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.release_management_detail (
  level_name                 STRING,
  issue_project              STRING,
  fix_version                STRING,
  level_value                STRING,
  user_release_date          STRING,
  user_start_date            STRING,
  start_date                 DATE,
  release_date               DATE,
  release_status             STRING,
  issue_completion_details   ARRAY<STRING>,
  total_commits              ARRAY<STRING>,
  total_prs                  ARRAY<STRING>,
  total_builds               ARRAY<STRING>,
  defect_density_details     ARRAY<STRING>,
  pipeline_trigger_details   ARRAY<STRING>,
  approval_gates             ARRAY<STRING>,
  vulnerabilities            ARRAY<STRING>,
  webapp_vulnerabilities     ARRAY<STRING>,
  bugs                       ARRAY<STRING>,
  tests                      ARRAY<STRING>
)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly'   = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants'   = 'supported',
  'delta.minReaderVersion'     = '3',
  'delta.minWriterVersion'     = '7')
"""

print("Creating release_management_detail...", end=" ")
spark.sql(ddl)
print("done")

count = spark.sql(
    f"SELECT COUNT(*) FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.release_management_detail"
).collect()[0][0]
print(f"  release_management_detail: {count} rows")
