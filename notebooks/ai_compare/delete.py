# AI Code Comparison dashboard — delete seeded rows.
#
# This batch is fully additive on top of direct/, so the delete must NOT
# wipe direct/'s 'github copilot' rows from the shared tables. We compute
# the non-copilot tool list from entities.yaml and scope deletes by
# tool-name on the shared tables.
#
# Scoping summary (every predicate is tightly demo-scoped):
#   - consumption_layer.ai_assistant_license_info        : access_level_name = demo-acme-direct (only this batch writes here)
#   - consumption_layer.ai_assistant_user_engagement     : level_name = demo-acme-direct        (only this batch writes here)
#   - consumption_layer.ai_assistant_programming_language_agg : level_type_name = demo-acme-direct
#   - consumption_layer.ai_assistant_language_model_metrics   : level_name = demo-acme-direct
#   - consumption_layer.commits_prs                      : org_name = demo-acme-direct
#   - consumption_layer.ai_assistant_acceptance_info     : level_name = demo-acme-direct AND ai_assistant_tool_name IN (<non-copilot tools>)
#   - consumption_layer.ai_code_assistant_usage_user_level    : org_name = demo-acme-direct AND ai_tool_name IN (<non-copilot tools>)
#   - master_data.filter_values_unity                    : created_by = seed-data-ai-compare@demo.io
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/ai_compare/delete.py").read())

import sys, os, yaml

# Module cache-bust so the helper functions resolve to current code
for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key in ("loader",):
        del sys.modules[_key]
sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

from generators import ai_compare_acceptance, ai_compare_usage_user_level

CATALOG  = "playground_prod"
TEST_ORG = "demo-acme-direct"

with open("config/entities.yaml") as f:
    entities = yaml.safe_load(f)

# Tool names owned by this batch (everything in entities['ai_tools'] except copilot).
NON_COPILOT_TOOLS = ai_compare_acceptance.tool_names(entities)
# Sanity: ai_compare_usage_user_level must agree.
assert NON_COPILOT_TOOLS == ai_compare_usage_user_level.tool_names(entities), \
    "non-copilot tool list mismatch between ai_compare generators"

_tool_list_sql = ", ".join(f"'{t}'" for t in NON_COPILOT_TOOLS) or "''"

# (schema.table, predicate)
_deletes = [
    # New tables — only this batch writes here, scope by demo org alone.
    ("consumption_layer.ai_assistant_license_info",
     f"access_level_name = '{TEST_ORG}'"),
    ("consumption_layer.ai_assistant_user_engagement",
     f"level_name = '{TEST_ORG}'"),
    ("consumption_layer.ai_assistant_programming_language_agg",
     f"level_type_name = '{TEST_ORG}'"),
    ("consumption_layer.ai_assistant_language_model_metrics",
     f"level_name = '{TEST_ORG}'"),
    ("consumption_layer.commits_prs",
     f"org_name = '{TEST_ORG}'"),
    # Shared tables with direct/ — scope by tool name so copilot rows survive.
    ("consumption_layer.ai_assistant_acceptance_info",
     f"level_name = '{TEST_ORG}' AND ai_assistant_tool_name IN ({_tool_list_sql})"),
    ("consumption_layer.ai_code_assistant_usage_user_level",
     f"org_name = '{TEST_ORG}' AND ai_tool_name IN ({_tool_list_sql})"),
    # filter_values_unity rows — distinct created_by so other dora/devex rows
    # in the same filter_group stay intact.
    ("master_data.filter_values_unity",
     "created_by = 'seed-data-ai-compare@demo.io'"),
]

for table, predicate in _deletes:
    fqn = f"{CATALOG}.{table}"
    try:
        if not spark.catalog.tableExists(fqn):
            print(f"{table}: skipped — table does not exist yet")
            continue
        n = spark.sql(f"SELECT COUNT(*) FROM {fqn} WHERE {predicate}").collect()[0][0]
        spark.sql(f"DELETE FROM {fqn} WHERE {predicate}")
        print(f"{table}: deleted {n} rows")
    except Exception as e:
        msg = str(e).split("\n")[0][:200]
        print(f"{table}: ERROR — {msg}")
