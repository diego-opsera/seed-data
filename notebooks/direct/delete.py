CATALOG  = "playground_prod"
SCHEMA   = "base_datasets"
TEST_ORG = "demo-acme-direct"   # only ever touches this org — core data is safe

tables = [
    "trf_github_copilot_direct_data",
    "commits_rest_api",
]

for table in tables:
    spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.{table} WHERE org_name = '{TEST_ORG}'")
    print(f"Deleted from {table}")

print("\nVerifying (should all be 0):")
for table in tables:
    n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table} WHERE org_name = '{TEST_ORG}'").collect()[0][0]
    print(f"  {table}: {n}")
