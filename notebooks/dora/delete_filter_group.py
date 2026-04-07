# Delete the demo-acme-corp filter_groups_unity row inserted by insert_filter_group.py
# Scoped strictly by createdBy = 'seed-data@demo.io' — will not touch any other rows.
# Run via exec(open("/tmp/seed-data/notebooks/dora/delete_filter_group.py").read())

import json

CATALOG = "playground_prod"
FGU = f"{CATALOG}.master_data.filter_groups_unity"

before = spark.sql(f"SELECT COUNT(*) AS n FROM {FGU} WHERE createdBy = 'seed-data@demo.io'").collect()[0]["n"]
spark.sql(f"DELETE FROM {FGU} WHERE createdBy = 'seed-data@demo.io'")
after = spark.sql(f"SELECT COUNT(*) AS n FROM {FGU} WHERE createdBy = 'seed-data@demo.io'").collect()[0]["n"]

print(json.dumps({"deleted": before - after, "remaining": after}))
