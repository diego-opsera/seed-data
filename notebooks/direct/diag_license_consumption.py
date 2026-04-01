CATALOG = "playground_prod"

print("=== License overview (should show ~150 allocated) ===")
spark.sql("WITH variables AS (SELECT DATE('2026-01-31') AS start_date, DATE('2026-03-31') AS end_date, DATEDIFF(DATE('2026-03-31'), DATE('2026-01-31')) + 1 AS dd), source AS (SELECT record_insert_datetime, cleansed_assignee_login, copilot_usage_date, org_name, CASE WHEN date(record_insert_datetime) BETWEEN (SELECT start_date FROM variables) AND (SELECT end_date FROM variables) THEN 'current period' WHEN date(record_insert_datetime) BETWEEN DATE_SUB((SELECT start_date FROM variables), (SELECT CAST(dd AS INT) FROM variables)) AND DATE_SUB((SELECT start_date FROM variables), 1) THEN 'previous period' END as time_period FROM playground_prod.base_datasets.v_github_copilot_seats_usage_user_level WHERE date(record_insert_datetime) BETWEEN DATE_SUB(DATE('2026-01-31'), DATEDIFF(DATE('2026-03-31'), DATE('2026-01-31')) + 1) AND DATE('2026-03-31') AND org_name = 'demo-acme-direct') SELECT time_period, COUNT(DISTINCT cleansed_assignee_login, org_name) as allocated_licenses FROM source GROUP BY time_period").show(5, False)

print("=== consumption_layer.commits_prs ===")
spark.sql("SELECT COUNT(*) as cnt FROM playground_prod.consumption_layer.commits_prs WHERE org_name = 'demo-acme-direct'").show()

print("=== ai_assistant_acceptance_info sample row for OpseraEngineering ===")
spark.sql("SELECT * FROM playground_prod.consumption_layer.ai_assistant_acceptance_info WHERE level_name = 'OpseraEngineering' LIMIT 3").show(3, False)

print("=== ai_code_assistant_usage_user_level columns ===")
spark.sql("DESCRIBE playground_prod.consumption_layer.ai_code_assistant_usage_user_level").show(30, False)
