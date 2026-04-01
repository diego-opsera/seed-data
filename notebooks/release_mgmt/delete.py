CATALOG         = "playground_prod"
SCHEMA          = "consumption_layer"
DEMO_ENTERPRISE = "demo-acme-corp"  # only ever touches this enterprise — real data is safe

table = "release_management_detail"

spark.sql(
    f"DELETE FROM {CATALOG}.{SCHEMA}.{table} WHERE level_value = '{DEMO_ENTERPRISE}'"
)
print(f"Deleted from {table} where level_value = '{DEMO_ENTERPRISE}'")

n = spark.sql(
    f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table} WHERE level_value = '{DEMO_ENTERPRISE}'"
).collect()[0][0]
print(f"  {table}: {n} rows remaining (should be 0)")
