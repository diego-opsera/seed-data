CATALOG  = "playground_prod"
TEST_ORG = "demo-acme-direct"

# Tables scoped by org_name = 'demo-acme-direct'
tables = [
    ("base_datasets",  "trf_github_copilot_direct_data"),
    ("base_datasets",  "commits_rest_api"),
    ("source_to_stage", "raw_github_copilot_seats"),
    ("master_data",    "github_copilot_orgs_mapping"),
    ("base_datasets",  "github_copilot_metrics_ide_org_level"),
]

for schema, table in tables:
    result = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    )
    n = result.collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

print("\nVerifying (should all be 0):")
for schema, table in tables:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"  {schema}.{table}: {n}")
