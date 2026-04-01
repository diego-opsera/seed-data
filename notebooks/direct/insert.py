import sys, os

# Flush any cached generator modules so re-runs always use the latest cloned code
for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key in ("loader",):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

import yaml
from generators import direct_data, commits, seats_usage, org_mapping, ide_org_level
from generators import code_scan_alert, secret_scan_alert, file_extensions, pull_requests

CATALOG = "playground_prod"

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

entities = load_yaml("config/entities.yaml")
story    = load_yaml("config/stories/narrative.yaml")

# Scope all direct-batch data to the test org so it can be wiped independently
# of the core batch (demo-acme-engineering). Switch orgs[0] to the direct org
# before passing entities to any generator.
entities_direct = {**entities, "orgs": [entities["orgs"][1]]}

statements = []
statements += [(direct_data.TABLE,      s) for s in direct_data.generate(CATALOG, entities_direct, story)]
statements += [(commits.TABLE,          s) for s in commits.generate(CATALOG, entities_direct, story)]
statements += [(seats_usage.TABLE,      s) for s in seats_usage.generate(CATALOG, entities, story)]
statements += [(org_mapping.TABLE,      s) for s in org_mapping.generate(CATALOG, entities, story)]
statements += [(ide_org_level.TABLE,    s) for s in ide_org_level.generate(CATALOG, entities_direct, story)]
statements += [(file_extensions.TABLE,  s) for s in file_extensions.generate(CATALOG, entities, story)]
statements += [(code_scan_alert.TABLE,  s) for s in code_scan_alert.generate(CATALOG, entities_direct, story)]
statements += [(secret_scan_alert.TABLE, s) for s in secret_scan_alert.generate(CATALOG, entities_direct, story)]
statements += [(pull_requests.TABLE,    s) for s in pull_requests.generate(CATALOG, entities_direct, story)]

for i, (table, sql) in enumerate(statements, 1):
    print(f"[{i}/{len(statements)}] {table}...", end=" ")
    spark.sql(sql)
    print("done")
