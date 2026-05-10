# Create the two raw_mongo_transformed_data_gitscraper tables that drive the
# Git Custodian widget on the Code Reliability dashboard. Both are missing
# from playground_prod entirely.
#
# Schema derived from gc_overview.sql + gc_trend.sql column references.
#
# Run once:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/create_table_gitscraper.py").read())
#
# Idempotent: CREATE TABLE IF NOT EXISTS.

CATALOG = "playground_prod"

DDL_SCANS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.source_to_stage.raw_mongo_transformed_data_gitscraper (
    _id           STRING,
    giturl        STRING,
    branch        STRING,
    pipelineName  STRING,
    pipelineId    STRING,
    runCount      INT,
    repository    STRING,
    totalIssues   INT,
    activityDate  TIMESTAMP,
    tags          ARRAY<STRUCT<type:STRING, value:STRING>>,
    record_inserted_by STRING,
    record_insert_datetime TIMESTAMP
)
USING DELTA
"""

DDL_ISSUES = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.source_to_stage.raw_mongo_transformed_data_gitscraper_issues (
    _id        STRING,
    lineNumber INT,
    path       STRING,
    reason     STRING,
    severity   STRING,
    status     STRING,
    author     STRING,
    email      STRING,
    record_inserted_by STRING,
    record_insert_datetime TIMESTAMP
)
USING DELTA
"""

print("Creating raw_mongo_transformed_data_gitscraper (idempotent)...")
spark.sql(DDL_SCANS)
print("Creating raw_mongo_transformed_data_gitscraper_issues (idempotent)...")
spark.sql(DDL_ISSUES)
print("Done.")

for t in [
    "raw_mongo_transformed_data_gitscraper",
    "raw_mongo_transformed_data_gitscraper_issues",
]:
    n = spark.sql(f"SELECT COUNT(*) n FROM {CATALOG}.source_to_stage.{t}").collect()[0]["n"]
    print(f"  {t}: {n} rows")
