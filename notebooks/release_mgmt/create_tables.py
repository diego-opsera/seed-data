TARGET_CATALOG = "playground_prod"
TARGET_SCHEMA  = "consumption_layer"

# Create schema if it doesn't already exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Schema {TARGET_CATALOG}.{TARGET_SCHEMA} ready.")

ddl = f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.release_management_detail (
  fix_version                STRING,
  issue_project              STRING,
  start_date                 DATE,
  release_date               DATE,
  release_status             STRING,
  level_name                 STRING,
  level_value                STRING,
  level_1                    STRING,
  level_2                    STRING,
  level_3                    STRING,
  level_4                    STRING,
  level_5                    STRING,
  kpi_uuids                  STRING,
  issue_completion_details   ARRAY<STRING>,
  pipeline_trigger_details   ARRAY<STRING>,
  total_prs                  ARRAY<STRING>,
  defect_density_details     ARRAY<STRING>,
  total_commits              ARRAY<STRING>,
  approval_gates             ARRAY<STRING>,
  vulnerabilities            ARRAY<STRING>,
  bugs                       ARRAY<STRING>,
  webapp_vulnerabilities     ARRAY<STRING>,
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

count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.release_management_detail").collect()[0][0]
print(f"  release_management_detail: {count} rows")
