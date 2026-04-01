print("=== jira_boards columns ===")
spark.sql("DESCRIBE playground_prod.base_datasets.jira_boards").show(30, False)

print("=== jira_boards sample rows ===")
spark.sql("SELECT * FROM playground_prod.base_datasets.jira_boards LIMIT 5").show(5, False)

print("=== distinct customer_ids in jira_boards ===")
spark.sql("SELECT DISTINCT customer_id FROM playground_prod.base_datasets.jira_boards LIMIT 20").show(20, False)

print("=== master_data.projects_table columns ===")
spark.sql("DESCRIBE playground_prod.master_data.projects_table").show(30, False)

print("=== master_data.projects_table sample rows ===")
spark.sql("SELECT * FROM playground_prod.master_data.projects_table LIMIT 5").show(5, False)
