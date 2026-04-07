import sys, os

# Flush cached generator modules so re-runs always use the latest cloned code
for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key in ("loader",):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

import yaml
from generators import dora

CATALOG = "playground_prod"

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

entities = load_yaml("config/entities.yaml")
story    = load_yaml("config/stories/narrative.yaml")

statements = dora.generate(CATALOG, entities, story)

for i, sql in enumerate(statements, 1):
    # Extract the target table name from the INSERT for the progress log
    tbl = sql.split("consumption_layer.")[1].split("\n")[0].strip()
    print(f"[{i}/{len(statements)}] {tbl}...", end=" ")
    spark.sql(sql)
    print("done")
