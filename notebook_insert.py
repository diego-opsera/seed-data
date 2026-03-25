import sys
import os

# -----------------------------------------------------------------------
# CONFIG — edit these
# -----------------------------------------------------------------------
CATALOG   = "opsera_test"
DRY_RUN   = True   # set to False to actually insert

# -----------------------------------------------------------------------
# Add repo to path so generators can be imported
# Update this path to wherever you've cloned the repo in the notebook env
# e.g. if you uploaded to DBFS: /dbfs/FileStore/seed-data
# -----------------------------------------------------------------------
REPO_PATH = "/path/to/seed-data"
sys.path.insert(0, REPO_PATH)
os.chdir(REPO_PATH)

# -----------------------------------------------------------------------
# Generate SQL
# -----------------------------------------------------------------------
import yaml
from generators import feature_level, ide_level, language_model_level, user_level, enterprise_level
from generators.utils import date_range, jitter, acceptance_subset

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

entities = load_yaml("config/entities.yaml")
story    = load_yaml("config/stories/baseline.yaml")

def build_user_row_dicts(entities, story):
    base  = story["per_user_per_day"]
    noise = story.get("noise_pct", 0)
    user_ide_map = story["user_ide_map"]
    languages    = entities["languages"]
    models       = entities["models"]
    rows = []
    for d in date_range(story["start_date"], story["end_date"]):
        for i, user in enumerate(entities["users"]):
            seed = hash((str(d), user["id"])) % 100000
            code_gen      = jitter(base["code_generation_activity_count"], noise, seed)
            code_acc      = acceptance_subset(code_gen, 0.45)
            loc_sugg_add  = jitter(base["loc_suggested_to_add"],    noise, seed + 1)
            loc_sugg_del  = jitter(base["loc_suggested_to_delete"],  noise, seed + 2)
            loc_add       = acceptance_subset(loc_sugg_add, 0.45)
            loc_del       = acceptance_subset(loc_sugg_del, 0.45)
            interactions  = jitter(base["user_initiated_interaction_count"], noise, seed + 3)
            rows.append({
                "usage_date":                       str(d),
                "user_id":                          user["id"],
                "ide":                              user_ide_map.get(user["login"], "vscode"),
                "language":                         languages[i % len(languages)],
                "model":                            models[i % len(models)],
                "code_generation_activity_count":   code_gen,
                "code_acceptance_activity_count":   code_acc,
                "loc_suggested_to_add_sum":         loc_sugg_add,
                "loc_suggested_to_delete_sum":      loc_sugg_del,
                "loc_added_sum":                    loc_add,
                "loc_deleted_sum":                  loc_del,
                "user_initiated_interaction_count": interactions,
                "used_chat":                        interactions > 0,
            })
    return rows

user_rows  = build_user_row_dicts(entities, story)
statements = []
statements += [(feature_level.TABLE,         s) for s in feature_level.generate(CATALOG, entities, story)]
statements += [(ide_level.TABLE,             s) for s in ide_level.generate(CATALOG, entities, story)]
statements += [(language_model_level.TABLE,  s) for s in language_model_level.generate(CATALOG, entities, story)]
statements += [(user_level.TABLE,            s) for s in user_level.generate(CATALOG, entities, story)]
statements += [(enterprise_level.TABLE,      s) for s in enterprise_level.generate(CATALOG, entities, story, user_rows)]

# -----------------------------------------------------------------------
# Preview
# -----------------------------------------------------------------------
print("Statements to execute:")
for table, sql in statements:
    n = sql.count("\n  (")
    print(f"  {table}: {n} row(s)")

if DRY_RUN:
    print("\nDRY_RUN=True — nothing inserted. Set DRY_RUN=False to execute.")
else:
    for i, (table, sql) in enumerate(statements, 1):
        print(f"[{i}/{len(statements)}] Inserting into {table}...", end=" ")
        spark.sql(sql)
        print("done")
    print("\nAll done.")
