import json

catalog = "opsera_prod"  # change this to your catalog name

queries = {
    "row_counts": f"""
SELECT 'enterprise_level_copilot_metrics' AS tbl, COUNT(*) AS rows, CAST(MIN(usage_date) AS STRING) AS earliest, CAST(MAX(usage_date) AS STRING) AS latest FROM {catalog}.base_datasets.enterprise_level_copilot_metrics
UNION ALL SELECT 'enterprise_user_level_copilot_metrics', COUNT(*), CAST(MIN(usage_date) AS STRING), CAST(MAX(usage_date) AS STRING) FROM {catalog}.base_datasets.enterprise_user_level_copilot_metrics
UNION ALL SELECT 'enterprise_user_feature_level_copilot_metrics', COUNT(*), CAST(MIN(usage_date) AS STRING), CAST(MAX(usage_date) AS STRING) FROM {catalog}.base_datasets.enterprise_user_feature_level_copilot_metrics
UNION ALL SELECT 'enterprise_user_ide_level_copilot_metrics', COUNT(*), CAST(MIN(usage_date) AS STRING), CAST(MAX(usage_date) AS STRING) FROM {catalog}.base_datasets.enterprise_user_ide_level_copilot_metrics
UNION ALL SELECT 'enterprise_user_language_model_level_copilot_metrics', COUNT(*), CAST(MIN(usage_date) AS STRING), CAST(MAX(usage_date) AS STRING) FROM {catalog}.base_datasets.enterprise_user_language_model_level_copilot_metrics
UNION ALL SELECT 'org_level_copilot_metrics', COUNT(*), CAST(MIN(usage_date) AS STRING), CAST(MAX(usage_date) AS STRING) FROM {catalog}.base_datasets.org_level_copilot_metrics
UNION ALL SELECT 'org_user_level_copilot_metrics', COUNT(*), CAST(MIN(usage_date) AS STRING), CAST(MAX(usage_date) AS STRING) FROM {catalog}.base_datasets.org_user_level_copilot_metrics
UNION ALL SELECT 'github_copilot_metrics_ide_org_level_new', COUNT(*), CAST(MIN(copilot_usage_date) AS STRING), CAST(MAX(copilot_usage_date) AS STRING) FROM {catalog}.base_datasets.github_copilot_metrics_ide_org_level_new
UNION ALL SELECT 'github_copilot_metrics_ide_teams_level_new', COUNT(*), CAST(MIN(copilot_usage_date) AS STRING), CAST(MAX(copilot_usage_date) AS STRING) FROM {catalog}.base_datasets.github_copilot_metrics_ide_teams_level_new
UNION ALL SELECT 'github_copilot_metrics_dotcom_org_level', COUNT(*), CAST(MIN(copilot_usage_date) AS STRING), CAST(MAX(copilot_usage_date) AS STRING) FROM {catalog}.base_datasets.github_copilot_metrics_dotcom_org_level
UNION ALL SELECT 'github_copilot_metrics_dotcom_teams_level', COUNT(*), CAST(MIN(copilot_usage_date) AS STRING), CAST(MAX(copilot_usage_date) AS STRING) FROM {catalog}.base_datasets.github_copilot_metrics_dotcom_teams_level
    """,
    "enterprise_ids": f"SELECT DISTINCT enterprise_id, enterprise FROM {catalog}.base_datasets.enterprise_level_copilot_metrics ORDER BY enterprise_id",
    "org_ids": f"SELECT DISTINCT organization_id, org_name FROM {catalog}.base_datasets.org_level_copilot_metrics ORDER BY organization_id",
    "language_model_enums": f"SELECT DISTINCT language, model FROM {catalog}.base_datasets.enterprise_user_language_model_level_copilot_metrics ORDER BY language, model",
    "nested_array_sample": f"SELECT totals_by_feature, totals_by_ide, totals_by_language_model, totals_by_language_feature, totals_by_model_feature FROM {catalog}.base_datasets.enterprise_user_level_copilot_metrics WHERE totals_by_feature IS NOT NULL LIMIT 1",
}

results = {}
for name, sql in queries.items():
    results[name] = [row.asDict(recursive=True) for row in spark.sql(sql).collect()]

print(json.dumps(results, indent=2, default=str))
