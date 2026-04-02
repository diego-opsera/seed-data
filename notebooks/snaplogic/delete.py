CATALOG = "playground_prod"
SCHEMA  = "source_to_stage"

# raw_snaplogic_snaplex and raw_snaplogic_snaplex_nodes filter on 'org'
for tbl in ["raw_snaplogic_snaplex", "raw_snaplogic_snaplex_nodes"]:
    spark.sql(
        f"DELETE FROM {CATALOG}.{SCHEMA}.{tbl} WHERE org = 'demo-acme-direct'"
    )
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{tbl} WHERE org = 'demo-acme-direct'"
    ).collect()[0][0]
    print(f"{tbl}: {n} demo rows remaining (should be 0)")

# raw_snaplogic_activities uses org_label instead of org
spark.sql(
    f"DELETE FROM {CATALOG}.{SCHEMA}.raw_snaplogic_activities"
    f" WHERE org_label = 'demo-acme-direct'"
)
n = spark.sql(
    f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.raw_snaplogic_activities"
    f" WHERE org_label = 'demo-acme-direct'"
).collect()[0][0]
print(f"raw_snaplogic_activities: {n} demo rows remaining (should be 0)")
