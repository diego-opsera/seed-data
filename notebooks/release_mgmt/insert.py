import sys, importlib

# Flush cached modules so a fresh git clone always takes effect
for mod in list(sys.modules.keys()):
    if mod.startswith("generators"):
        del sys.modules[mod]

sys.path.insert(0, "/tmp/seed-data")

import yaml
from generators import release_management

with open("/tmp/seed-data/config/entities.yaml") as f:
    entities = yaml.safe_load(f)
with open("/tmp/seed-data/config/stories/narrative.yaml") as f:
    story = yaml.safe_load(f)

CATALOG = "playground_prod"

statements = release_management.generate(CATALOG, entities, story)

print(f"Inserting {len(statements)} rows into consumption_layer.release_management_detail...")
for i, stmt in enumerate(statements):
    spark.sql(stmt)
    if (i + 1) % 8 == 0:
        print(f"  {i + 1}/{len(statements)} done")

print(f"\nAll {len(statements)} rows inserted.")

n = spark.sql(
    "SELECT COUNT(*) FROM playground_prod.consumption_layer.release_management_detail"
    " WHERE fix_version LIKE 'demo-%'"
).collect()[0][0]
print(f"Verification: {n} rows with fix_version LIKE 'demo-%'")
