import sys, os

# Flush any cached generator modules so re-runs always use the latest cloned code
for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key in ("loader",):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

import yaml
from generators import direct_data, seats_usage, ide_org_level, commits

CATALOG = "playground_prod"

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

entities = load_yaml("config/entities.yaml")
story    = load_yaml("config/stories/narrative.yaml")

statements = []
statements += [(direct_data.TABLE,   s) for s in direct_data.generate(CATALOG, entities, story)]
statements += [(seats_usage.TABLE,   s) for s in seats_usage.generate(CATALOG, entities, story)]
statements += [(ide_org_level.TABLE, s) for s in ide_org_level.generate(CATALOG, entities, story)]
statements += [(commits.TABLE,       s) for s in commits.generate(CATALOG, entities, story)]

for i, (table, sql) in enumerate(statements, 1):
    print(f"[{i}/{len(statements)}] {table}...", end=" ")
    spark.sql(sql)
    print("done")
