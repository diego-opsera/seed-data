import json

catalog = "your_playground_catalog"  # change this

tables = [
    "enterprise_level_copilot_metrics",
    "enterprise_user_feature_level_copilot_metrics",
    "enterprise_user_ide_level_copilot_metrics",
    "enterprise_user_language_model_level_copilot_metrics",
]

results = {}
for t in tables:
    rows = spark.sql(f"DESCRIBE TABLE {catalog}.base_datasets.{t}").collect()
    results[t] = {r["col_name"]: r["data_type"] for r in rows if r["col_name"] and not r["col_name"].startswith("#")}

print(json.dumps(results, indent=2))
