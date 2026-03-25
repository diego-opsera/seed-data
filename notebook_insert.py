import sys
import os

# Update this to your Repos path — e.g. /Workspace/Repos/you@company.com/seed-data
REPO_PATH = "/Workspace/Repos/your-email@company.com/seed-data"
CATALOG   = "playground_prod"
STORY     = "narrative"

sys.path.insert(0, REPO_PATH)
os.chdir(REPO_PATH)

import yaml
from generators import feature_level, ide_level, language_model_level, user_level, enterprise_level
from generators.user_level import build_user_row_dicts

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

entities = load_yaml("config/entities.yaml")
story    = load_yaml(f"config/stories/{STORY}.yaml")

user_rows  = build_user_row_dicts(entities, story)
statements = []
statements += [(feature_level.TABLE,        s) for s in feature_level.generate(CATALOG, entities, story)]
statements += [(ide_level.TABLE,            s) for s in ide_level.generate(CATALOG, entities, story)]
statements += [(language_model_level.TABLE, s) for s in language_model_level.generate(CATALOG, entities, story)]
statements += [(user_level.TABLE,           s) for s in user_level.generate(CATALOG, entities, story)]
statements += [(enterprise_level.TABLE,     s) for s in enterprise_level.generate(CATALOG, entities, story, user_rows)]

print(f"Story: {STORY} | Catalog: {CATALOG}")
for table, sql in statements:
    n = sql.count("\n  (")
    print(f"  {table}: {n} rows")

print("\nInserting...")
for i, (table, sql) in enumerate(statements, 1):
    print(f"[{i}/{len(statements)}] {table}...", end=" ")
    spark.sql(sql)
    print("done")

print("\nVerifying row counts:")
for table, _ in statements:
    n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.base_datasets.{table} WHERE enterprise_id = 999999").collect()[0][0]
    print(f"  {table}: {n}")
