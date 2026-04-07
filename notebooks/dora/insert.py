import sys, os

for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key in ("loader",):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

import yaml
from generators import dora, dora_charts

CATALOG = "playground_prod"

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

entities = load_yaml("config/entities.yaml")
story    = load_yaml("config/stories/narrative.yaml")

all_statements = dora.generate(CATALOG, entities, story) + dora_charts.generate(CATALOG, entities, story)

for i, sql in enumerate(all_statements, 1):
    # Derive a short label for progress output
    if "pipeline_activities" in sql:
        tbl = "pipeline_activities"
    elif "cfr_mttr_metric_data" in sql:
        tbl = "cfr_mttr_metric_data"
    elif "pipeline_deployment_commits" in sql:
        tbl = "pipeline_deployment_commits"
    else:
        tbl = sql.split("consumption_layer.")[1].split("\n")[0].strip()
    print(f"[{i}/{len(all_statements)}] {tbl}...", end=" ")
    spark.sql(sql)
    print("done")
