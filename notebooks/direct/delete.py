CATALOG = "playground_prod"
SCHEMA = "base_datasets"

tables = [
    "trf_github_copilot_direct_data",
    "v_github_copilot_seats_usage_user_level",
    "v_github_copilot_metrics_ide_org_level",
    "commits_rest_api",
]

for table in tables:
    spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.{table} WHERE enterprise_id = 999999")
    print(f"Deleted from {table}")

print("\nVerifying (should all be 0):")
for table in tables:
    n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table} WHERE enterprise_id = 999999").collect()[0][0]
    print(f"  {table}: {n}")
