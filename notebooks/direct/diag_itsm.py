print("=== v_itsm_issues_current row count for demo-acme-direct ===")
spark.sql("SELECT COUNT(*) FROM playground_prod.base_datasets.v_itsm_issues_current WHERE customer_id = 'demo-acme-direct'").show()

print("=== v_itsm_issues_current sample row for demo-acme-direct ===")
spark.sql("SELECT customer_id, issue_key, issue_type, issue_status, sprint_name, story_points FROM playground_prod.base_datasets.v_itsm_issues_current WHERE customer_id = 'demo-acme-direct' LIMIT 3").show(3, False)

print("=== Check if there is a jira_projects or itsm_org_mapping table ===")
spark.sql("SHOW TABLES IN playground_prod.master_data").show(50, False)

print("=== Check master_data for any customer_id mapping ===")
spark.sql("SHOW TABLES IN playground_prod.base_datasets LIKE '*itsm*'").show(20, False)
spark.sql("SHOW TABLES IN playground_prod.base_datasets LIKE '*jira*'").show(20, False)
spark.sql("SHOW TABLES IN playground_prod.consumption_layer LIKE '*itsm*'").show(20, False)
spark.sql("SHOW TABLES IN playground_prod.consumption_layer LIKE '*jira*'").show(20, False)
