print("=== mt_itsm_issues_current columns ===")
spark.sql("DESCRIBE playground_prod.transform_stage.mt_itsm_issues_current").show(60, False)

print("=== mt_itsm_issues_current sample row ===")
spark.sql("SELECT * FROM playground_prod.transform_stage.mt_itsm_issues_current LIMIT 1").show(1, False)
