CATALOG  = "playground_prod"
TEST_ORG = "demo-acme-direct"

# pull_requests: scoped to seeded IDs — never touches other orgs' rows
n = spark.sql(
    f"DELETE FROM {CATALOG}.base_datasets.pull_requests "
    f"WHERE merge_request_id LIKE 'demo-seed-pr-%'"
).collect()[0][0]
print(f"Deleted {n} rows from base_datasets.pull_requests")

# commits_rest_api
n = spark.sql(
    f"DELETE FROM {CATALOG}.base_datasets.commits_rest_api WHERE org_name = '{TEST_ORG}'"
).collect()[0][0]
print(f"Deleted {n} rows from base_datasets.commits_rest_api")

# github_teams_members
n = spark.sql(
    f"DELETE FROM {CATALOG}.source_to_stage.raw_github_teams_members WHERE org_name = '{TEST_ORG}'"
).collect()[0][0]
print(f"Deleted {n} rows from source_to_stage.raw_github_teams_members")

# servicenow change requests: scoped to seeded IDs (table may not exist on first run)
try:
    n = spark.sql(
        f"DELETE FROM {CATALOG}.transform_stage.trf_servicenow_change_requests "
        f"WHERE issue_key LIKE 'demo-seed-chg-%'"
    ).collect()[0][0]
    print(f"Deleted {n} rows from transform_stage.trf_servicenow_change_requests")
except Exception as e:
    print(f"trf_servicenow_change_requests: skipped ({e})")

# itsm_issues (feeds both devex feature delivery rate AND dora CTFC)
for schema, table in [
    ("transform_stage", "mt_itsm_issues_current"),
    ("transform_stage", "mt_itsm_issues_hist"),
]:
    n = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE customer_id = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

# filter group + filter values — scoped to 'seed-data@devex.io' only
# (dora filter rows use 'seed-data@demo.io' and are not touched here)
try:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.master_data.filter_groups_unity "
        f"WHERE createdBy = 'seed-data@devex.io'"
    ).collect()[0][0]
    spark.sql(f"DELETE FROM {CATALOG}.master_data.filter_groups_unity WHERE createdBy = 'seed-data@devex.io'")
    print(f"filter_groups_unity: deleted {n} rows")
except Exception as e:
    print(f"filter_groups_unity: ERROR — {e}")

try:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.master_data.filter_values_unity "
        f"WHERE created_by = 'seed-data@devex.io'"
    ).collect()[0][0]
    spark.sql(f"DELETE FROM {CATALOG}.master_data.filter_values_unity WHERE created_by = 'seed-data@devex.io'")
    print(f"filter_values_unity: deleted {n} rows")
except Exception as e:
    print(f"filter_values_unity: ERROR — {e}")

# Verification (should all be 0)
print("\nVerifying (should all be 0):")
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.base_datasets.pull_requests WHERE merge_request_id LIKE 'demo-seed-pr-%'").collect()[0][0]
print(f"  pull_requests (demo-seed-pr-*): {n}")
try:
    n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.transform_stage.trf_servicenow_change_requests WHERE issue_key LIKE 'demo-seed-chg-%'").collect()[0][0]
    print(f"  trf_servicenow_change_requests (demo-seed-chg-*): {n}")
except Exception:
    print(f"  trf_servicenow_change_requests: table not yet created")
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.base_datasets.commits_rest_api WHERE org_name = '{TEST_ORG}'").collect()[0][0]
print(f"  commits_rest_api: {n}")
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.source_to_stage.raw_github_teams_members WHERE org_name = '{TEST_ORG}'").collect()[0][0]
print(f"  raw_github_teams_members: {n}")
for schema, table in [("transform_stage", "mt_itsm_issues_current"), ("transform_stage", "mt_itsm_issues_hist")]:
    n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table} WHERE customer_id = '{TEST_ORG}'").collect()[0][0]
    print(f"  {schema}.{table}: {n}")
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.master_data.filter_groups_unity WHERE createdBy = 'seed-data@devex.io'").collect()[0][0]
print(f"  filter_groups_unity (devex): {n}")
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.master_data.filter_values_unity WHERE created_by = 'seed-data@devex.io'").collect()[0][0]
print(f"  filter_values_unity (devex): {n}")
