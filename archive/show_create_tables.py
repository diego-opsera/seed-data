source_catalog = "opsera_prod"  # change this
source_schema = "base_datasets"  # change if needed

tables = [
    "enterprise_level_copilot_metrics",
    "enterprise_user_feature_level_copilot_metrics",
    "enterprise_user_ide_level_copilot_metrics",
    "enterprise_user_language_model_level_copilot_metrics",
    "enterprise_user_level_copilot_metrics",
]

for t in tables:
    print(f"\n{'='*60}\n{t}\n{'='*60}")
    result = spark.sql(f"SHOW CREATE TABLE {source_catalog}.{source_schema}.{t}").collect()
    print(result[0][0])
