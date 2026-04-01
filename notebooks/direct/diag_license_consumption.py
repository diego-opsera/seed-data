CATALOG = "playground_prod"

print("=== raw_github_copilot_billing columns ===")
for col in spark.sql("SELECT * FROM playground_prod.source_to_stage.raw_github_copilot_billing LIMIT 0").columns:
    print(" ", col)

print("=== raw_github_copilot_billing row count for demo-acme-direct ===")
spark.sql("SELECT COUNT(*) FROM playground_prod.source_to_stage.raw_github_copilot_billing WHERE org_name = 'demo-acme-direct'").show()

print("=== v_github_copilot_seats_billing row count for demo-acme-direct ===")
spark.sql("SELECT COUNT(*) FROM playground_prod.base_datasets.v_github_copilot_seats_billing WHERE org_name = 'demo-acme-direct'").show()

print("=== v_github_copilot_seats_billing sample rows ===")
spark.sql("SELECT org_name, seat_breakdown_total, seat_breakdown_active_this_cycle, copilot_usage_date FROM playground_prod.base_datasets.v_github_copilot_seats_billing WHERE org_name = 'demo-acme-direct' LIMIT 5").show(5, False)

print("=== v_copilot_developer_activity_org columns ===")
for col in spark.sql("SELECT * FROM playground_prod.consumption_layer.v_copilot_developer_activity_org LIMIT 0").columns:
    print(" ", col)

print("=== v_copilot_developer_activity_org row count for demo-acme-direct ===")
spark.sql("SELECT COUNT(*) FROM playground_prod.consumption_layer.v_copilot_developer_activity_org WHERE org_name = 'demo-acme-direct'").show()

print("=== v_copilot_developer_activity_org view definition ===")
spark.sql("DESCRIBE EXTENDED playground_prod.consumption_layer.v_copilot_developer_activity_org").filter("col_name = 'View Text'").show(1, False)

print("=== pr_metric_view view definition ===")
spark.sql("DESCRIBE EXTENDED playground_prod.consumption_layer.pr_metric_view").filter("col_name = 'View Text'").show(1, False)

print("=== pr_metric_view sample rows (any) ===")
spark.sql("SELECT project_name, merge_request_id, pr_source, pr_state FROM playground_prod.consumption_layer.pr_metric_view LIMIT 5").show(5, False)
