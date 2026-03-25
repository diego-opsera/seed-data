import os

REPO_PATH = "/path/to/seed-data"  # update this to wherever you cloned/uploaded the repo
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
    path = os.path.join(REPO_PATH, "payloads", f"baseline__{table}.sql")
    with open(path) as f:
        sql = f.read().replace("opsera_test.base_datasets", f"{CATALOG}.{SCHEMA}")
    print(f"Inserting into {table}...", end=" ")
    spark.sql(sql)
    print("done")

print("\nVerifying row counts:")
for table in tables:
    n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table} WHERE enterprise_id = 999999").collect()[0][0]
    print(f"  {table}: {n}")
