print("=== v_itsm_issues_current view definition ===")
spark.sql("DESCRIBE EXTENDED playground_prod.base_datasets.v_itsm_issues_current").filter("col_name = 'View Text'").show(1, False)

print("=== v_itsm_issues_current sample rows (any org) ===")
spark.sql("SELECT customer_id, issue_type, issue_project, issue_status, story_points, sprint_name FROM playground_prod.base_datasets.v_itsm_issues_current LIMIT 5").show(5, False)

print("=== distinct customer_ids in v_itsm_issues_current ===")
spark.sql("SELECT DISTINCT customer_id FROM playground_prod.base_datasets.v_itsm_issues_current LIMIT 20").show(20, False)
