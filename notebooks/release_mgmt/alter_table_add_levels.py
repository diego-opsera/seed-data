"""
One-time schema migration: add level_1 through level_5 columns to
consumption_layer.release_management_detail.

The API's release management SQL templates use:
  WHERE level_1 IN ('ProjectName')
treating level_1 as a direct column name. The table was created with
level_name / level_value columns instead. This ALTER TABLE adds the
missing columns so dashboard queries can find data.

Run ONCE. Safe to re-run (IF NOT EXISTS protects against duplicate columns).
Existing rows get NULL for the new columns; generators now INSERT level_1 values.
"""

CATALOG = "playground_prod"

print("Adding level_1–level_5 columns to release_management_detail...")
try:
    spark.sql(f"""
        ALTER TABLE {CATALOG}.consumption_layer.release_management_detail
        ADD COLUMNS (
            level_1 STRING,
            level_2 STRING,
            level_3 STRING,
            level_4 STRING,
            level_5 STRING
        )
    """)
    print("ALTER TABLE done")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Columns already exist — skipping: {e}")
    else:
        raise

print("\nVerify schema:")
spark.sql(f"""
    SELECT col_name, data_type
    FROM (DESCRIBE {CATALOG}.consumption_layer.release_management_detail)
    WHERE col_name IN ('level_name','level_value','level_1','level_2','level_3','level_4','level_5')
""").show(10, False)
