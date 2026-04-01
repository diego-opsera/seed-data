CATALOG = "playground_prod"

print("=== v_github_copilot_seats_billing columns ===")
for col in spark.sql("SELECT * FROM playground_prod.base_datasets.v_github_copilot_seats_billing LIMIT 0").columns:
    print(" ", col)

billing_cols = spark.sql("SELECT * FROM playground_prod.base_datasets.v_github_copilot_seats_billing LIMIT 0").columns
billing_org_col = "org_name" if "org_name" in billing_cols else ("organization" if "organization" in billing_cols else None)
print("=== v_github_copilot_seats_billing org col: " + str(billing_org_col) + " ===")
if billing_org_col:
    spark.sql("SELECT COUNT(*) FROM playground_prod.base_datasets.v_github_copilot_seats_billing WHERE " + billing_org_col + " = 'demo-acme-direct'").show()

print("=== raw_github_copilot_billing columns ===")
for col in spark.sql("SELECT * FROM playground_prod.source_to_stage.raw_github_copilot_billing LIMIT 0").columns:
    print(" ", col)

print("=== raw_github_copilot_billing row count for demo-acme-direct ===")
spark.sql("SELECT COUNT(*) FROM playground_prod.source_to_stage.raw_github_copilot_billing WHERE org_name = 'demo-acme-direct'").show()

print("=== consumption_layer view columns ===")
for view in ["copilot_org_info_metric_view", "copilot_developer_activity_org_metric_view", "pr_metric_view"]:
    cols = spark.sql("SELECT * FROM playground_prod.consumption_layer." + view + " LIMIT 0").columns
    print(view + ": " + str(cols))

print("=== consumption_layer view row counts ===")
for view in ["copilot_org_info_metric_view", "copilot_developer_activity_org_metric_view", "pr_metric_view"]:
    cols = spark.sql("SELECT * FROM playground_prod.consumption_layer." + view + " LIMIT 0").columns
    org_col = "org_name" if "org_name" in cols else ("organization" if "organization" in cols else None)
    if org_col:
        cnt = spark.sql("SELECT COUNT(*) FROM playground_prod.consumption_layer." + view + " WHERE " + org_col + " = 'demo-acme-direct'").collect()[0][0]
        print(view + " [" + org_col + "]: " + str(cnt))
    else:
        print(view + ": no org_name/organization column, columns are: " + str(cols))

print("=== pr_metric_view view definition ===")
spark.sql("DESCRIBE EXTENDED playground_prod.consumption_layer.pr_metric_view").filter("col_name = 'View Text'").show(1, False)

print("=== pr_metric_view sample rows ===")
spark.sql("SELECT * FROM playground_prod.consumption_layer.pr_metric_view LIMIT 3").show(3, False)

print("=== copilot_org_info_metric_view view definition ===")
spark.sql("DESCRIBE EXTENDED playground_prod.consumption_layer.copilot_org_info_metric_view").filter("col_name = 'View Text'").show(1, False)

print("=== copilot_developer_activity_org_metric_view definition ===")
spark.sql("DESCRIBE EXTENDED playground_prod.consumption_layer.copilot_developer_activity_org_metric_view").filter("col_name = 'View Text'").show(1, False)

print("=== v_github_copilot_seats_billing view definition ===")
spark.sql("DESCRIBE EXTENDED playground_prod.base_datasets.v_github_copilot_seats_billing").filter("col_name = 'View Text'").show(1, False)

print("=== License overview (should show ~150 allocated) ===")
spark.sql("WITH variables AS (SELECT DATE('2026-01-31') AS start_date, DATE('2026-03-31') AS end_date, DATEDIFF(DATE('2026-03-31'), DATE('2026-01-31')) + 1 AS dd), source AS (SELECT record_insert_datetime, cleansed_assignee_login, copilot_usage_date, org_name, CASE WHEN date(record_insert_datetime) BETWEEN (SELECT start_date FROM variables) AND (SELECT end_date FROM variables) THEN 'current period' WHEN date(record_insert_datetime) BETWEEN DATE_SUB((SELECT start_date FROM variables), (SELECT CAST(dd AS INT) FROM variables)) AND DATE_SUB((SELECT start_date FROM variables), 1) THEN 'previous period' END as time_period FROM playground_prod.base_datasets.v_github_copilot_seats_usage_user_level WHERE date(record_insert_datetime) BETWEEN DATE_SUB(DATE('2026-01-31'), DATEDIFF(DATE('2026-03-31'), DATE('2026-01-31')) + 1) AND DATE('2026-03-31') AND org_name = 'demo-acme-direct') SELECT time_period, COUNT(DISTINCT cleansed_assignee_login, org_name) as allocated_licenses FROM source GROUP BY time_period").show(5, False)

print("=== consumption_layer.commits_prs ===")
spark.sql("SELECT COUNT(*) as cnt FROM playground_prod.consumption_layer.commits_prs WHERE org_name = 'demo-acme-direct'").show()

print("=== ai_assistant_acceptance_info sample row for demo-acme-direct ===")
spark.sql("SELECT * FROM playground_prod.consumption_layer.ai_assistant_acceptance_info WHERE level_name = 'demo-acme-direct' LIMIT 3").show(3, False)

print("=== ai_code_assistant_usage_user_level columns ===")
spark.sql("DESCRIBE playground_prod.consumption_layer.ai_code_assistant_usage_user_level").show(30, False)
