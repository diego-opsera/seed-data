CATALOG = "playground_prod"
SCHEMA = "base_datasets"

tables = [
    "enterprise_user_feature_level_copilot_metrics",
    "enterprise_user_ide_level_copilot_metrics",
    "enterprise_user_language_model_level_copilot_metrics",
    "enterprise_user_level_copilot_metrics",
    "enterprise_level_copilot_metrics",
]

for table in tables:
    spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.{table} WHERE enterprise_id = 999999")
    print(f"Deleted from {table}")

print("\nVerifying (should all be 0):")
for table in tables:
    n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table} WHERE enterprise_id = 999999").collect()[0][0]
    print(f"  {table}: {n}")
