# Delete all config rows inserted by insert_filter_group.py.
# Scoped strictly by created_by / createdBy = 'seed-data@demo.io'.
# Run via exec(open("/tmp/seed-data/notebooks/dora/delete_filter_group.py").read())

import json

CATALOG = "playground_prod"

def _del(table, col, val):
    before = spark.sql(f"SELECT COUNT(*) AS n FROM {table} WHERE {col} = '{val}'").collect()[0]["n"]
    spark.sql(f"DELETE FROM {table} WHERE {col} = '{val}'")
    after  = spark.sql(f"SELECT COUNT(*) AS n FROM {table} WHERE {col} = '{val}'").collect()[0]["n"]
    return {"table": table.split(".")[-1], "deleted": before - after, "remaining": after}

results = [
    _del(f"{CATALOG}.master_data.filter_groups_unity",  "createdBy",   "seed-data@demo.io"),
    _del(f"{CATALOG}.master_data.filter_values_unity",  "created_by",  "seed-data@demo.io"),
]
print(json.dumps(results, indent=2))
