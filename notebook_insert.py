import sys, os
sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

import yaml
from generators import feature_level, ide_level, language_model_level, user_level, enterprise_level
from generators.user_level import build_user_row_dicts

CATALOG = "playground_prod"

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

entities = load_yaml("config/entities.yaml")
story    = load_yaml("config/stories/narrative.yaml")

user_rows  = build_user_row_dicts(entities, story)
statements = []
statements += [(feature_level.TABLE,        s) for s in feature_level.generate(CATALOG, entities, story)]
statements += [(ide_level.TABLE,            s) for s in ide_level.generate(CATALOG, entities, story)]
statements += [(language_model_level.TABLE, s) for s in language_model_level.generate(CATALOG, entities, story)]
statements += [(user_level.TABLE,           s) for s in user_level.generate(CATALOG, entities, story)]
statements += [(enterprise_level.TABLE,     s) for s in enterprise_level.generate(CATALOG, entities, story, user_rows)]

for i, (table, sql) in enumerate(statements, 1):
    print(f"[{i}/{len(statements)}] {table}...", end=" ")
    spark.sql(sql)
    print("done")
