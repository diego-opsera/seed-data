# ONE-TIME cleanup: deletes filter rows inserted under wrong KPI UUID (0cb8981e =
# jira_velocity_table_data, not CTFC) from the demo-acme-corp filter group.
# Only needed in environments where insert_ctfc_filter.py ran before the UUID fix.
# After running this, run insert_ctfc_filter_v2.py to insert the correct rows.
# Run via exec(open("/tmp/seed-data/notebooks/dora/cleanup_ctfc_wrong_kpi.py").read())

CATALOG = "playground_prod"
FILTER_GROUP_ID = "d277535f-a8cb-4429-965d-a9de685b4045"  # demo-acme-corp
WRONG_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"

before = spark.sql(f"""
    SELECT COUNT(*) FROM {CATALOG}.master_data.filter_values_unity
    WHERE filter_group_id = '{FILTER_GROUP_ID}'
      AND array_contains(kpi_uuids, '{WRONG_KPI}')
      AND created_by = 'seed-data@demo.io'
""").collect()[0][0]

spark.sql(f"""
    DELETE FROM {CATALOG}.master_data.filter_values_unity
    WHERE filter_group_id = '{FILTER_GROUP_ID}'
      AND array_contains(kpi_uuids, '{WRONG_KPI}')
      AND created_by = 'seed-data@demo.io'
""")

after = spark.sql(f"""
    SELECT COUNT(*) FROM {CATALOG}.master_data.filter_values_unity
    WHERE filter_group_id = '{FILTER_GROUP_ID}'
      AND array_contains(kpi_uuids, '{WRONG_KPI}')
      AND created_by = 'seed-data@demo.io'
""").collect()[0][0]

print(f"filter_values_unity (wrong KPI): {before} → {after} rows (should be 0)")
