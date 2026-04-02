import sys

for _key in list(sys.modules.keys()):
    if _key.startswith("generators"):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")

import yaml
from generators import snaplogic

with open("/tmp/seed-data/config/entities.yaml") as f:
    entities = yaml.safe_load(f)
with open("/tmp/seed-data/config/stories/narrative.yaml") as f:
    story = yaml.safe_load(f)

CATALOG = "playground_prod"

statements = (
    [(snaplogic.TABLE_SNAPLEX,    s) for s in snaplogic.generate_snaplex(CATALOG, entities, story)]
  + [(snaplogic.TABLE_NODES,      s) for s in snaplogic.generate_nodes(CATALOG, entities, story)]
  + [(snaplogic.TABLE_ACTIVITIES, s) for s in snaplogic.generate_activities(CATALOG, entities, story)]
)

for i, (table, sql) in enumerate(statements, 1):
    print(f"[{i}/{len(statements)}] {table}...", end=" ")
    spark.sql(sql)
    print("done")

# Verify
for tbl, col in [
    ("raw_snaplogic_snaplex",       "org"),
    ("raw_snaplogic_snaplex_nodes", "org"),
    ("raw_snaplogic_activities",    "org_label"),
]:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.source_to_stage.{tbl}"
        f" WHERE {col} = 'demo-acme-direct'"
    ).collect()[0][0]
    print(f"  {tbl}: {n:,} demo rows")
