CATALOG  = "playground_prod"
TEST_ORG = "demo-acme-direct"

# Tables scoped by org_name = 'demo-acme-direct'
org_scoped_tables = [
    ("base_datasets",   "trf_github_copilot_direct_data"),
    ("base_datasets",   "commits_rest_api"),
    ("source_to_stage", "raw_github_copilot_seats"),
    ("master_data",     "github_copilot_orgs_mapping"),
    ("base_datasets",   "github_copilot_metrics_ide_org_level"),
    ("base_datasets",   "code_scan_alert"),
    ("base_datasets",   "secret_scan_alert"),
]

# pull_requests is shared — only delete rows we seeded (scoped by merge_request_id prefix)
result = spark.sql(
    f"DELETE FROM {CATALOG}.base_datasets.pull_requests "
    f"WHERE merge_request_id LIKE 'demo-seed-pr-%'"
)
print(f"Deleted {result.collect()[0][0]} rows from base_datasets.pull_requests")


for schema, table in org_scoped_tables:
    result = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    )
    n = result.collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

# file_extensions is a shared reference table cloned from opsera_test.
# The insert uses MERGE (insert-if-not-exists), so pre-existing rows are
# never duplicated and never ours to delete. No cleanup needed here.
print("Skipped master_data.file_extensions (shared reference table — MERGE insert only)")

print("\nVerifying (should all be 0):")
for schema, table in org_scoped_tables:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"  {schema}.{table}: {n}")
print(f"  master_data.file_extensions: (not cleaned — shared reference table)")
n = spark.sql(
    f"SELECT COUNT(*) FROM {CATALOG}.base_datasets.pull_requests "
    f"WHERE merge_request_id LIKE 'demo-seed-pr-%'"
).collect()[0][0]
print(f"  base_datasets.pull_requests (demo-seed-pr-* only): {n}")
