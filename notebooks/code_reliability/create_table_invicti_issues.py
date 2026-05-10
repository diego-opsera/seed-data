# Create source_to_stage.raw_invicti_all_issues — missing from playground_prod.
#
# was_overview.sql LEFT JOINs this table on WebsiteId and filters on
# a.Severity + a.IsPresent in the WHERE clause, which means a missing
# table forces the WAS widget to return zero rows AND throws an FE error.
# Creating it with the columns the SQL reads, plus a few "look real" extras.
#
# Run once:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/create_table_invicti_issues.py").read())
#
# Idempotent: CREATE TABLE IF NOT EXISTS.

CATALOG = "playground_prod"
SCHEMA  = "source_to_stage"
TABLE   = "raw_invicti_all_issues"
FQN     = f"{CATALOG}.{SCHEMA}.{TABLE}"

DDL = f"""
CREATE TABLE IF NOT EXISTS {FQN} (
    WebsiteId       STRING,
    WebsiteName     STRING,
    Severity        STRING,
    State           STRING,
    IsPresent       BOOLEAN,
    Title           STRING,
    Url             STRING,
    Type            STRING,
    Certainty       STRING,
    LastSeenDate    STRING,
    FirstSeenDate   STRING,
    AssigneeName    STRING,
    Description     STRING,
    RemediationDescription STRING,
    Impact          STRING,
    LookupId        STRING,
    record_inserted_by STRING,
    record_insert_datetime TIMESTAMP
)
USING DELTA
"""

print(f"Creating {FQN} (idempotent)...")
spark.sql(DDL)
print("Done.")

# Sanity: confirm row count is 0 (or whatever exists from prior runs)
n = spark.sql(f"SELECT COUNT(*) AS n FROM {FQN}").collect()[0]["n"]
print(f"{FQN} row count: {n}")
