CATALOG = "playground_prod"
SCHEMA  = "consumption_layer"

# Scoped by fix_version prefix — safe even when level_value matches real project IDs
table = "release_management_detail"

spark.sql(
    f"DELETE FROM {CATALOG}.{SCHEMA}.{table} WHERE fix_version LIKE 'demo-%'"
)
print(f"Deleted from {table} where fix_version LIKE 'demo-%'")

n = spark.sql(
    f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table} WHERE fix_version LIKE 'demo-%'"
).collect()[0][0]
print(f"  {table}: {n} rows remaining (should be 0)")
