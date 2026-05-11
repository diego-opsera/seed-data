# AI Code Comparison dashboard — seed the consumption-layer + filter rows
# needed for charts under /insights/ai-code-comparison.
#
# This batch is fully additive: direct/ owns the 'github copilot' rows in
# ai_assistant_acceptance_info + ai_code_assistant_usage_user_level, and
# this batch adds the cursor + claude-code rows alongside them. Deletes
# here scope by ai_tool_name IN (<non-copilot tools>) so re-running this
# batch never touches direct/'s rows.
#
# Run order: AFTER direct/insert.py + dora/insert.py.
#   - direct/insert.py owns the github copilot rows in acceptance_info +
#     usage_user_level.
#   - dora/insert.py creates the Acme filter_group whose filter_group_id we
#     extend here with tool_type=cursor / tool_type='claude code' rows.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/ai_compare/insert.py").read())

import sys, os, uuid, yaml

for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key in ("loader",):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

from generators import (
    ai_assistant_license_info,
    ai_assistant_user_engagement,
    ai_assistant_programming_language_agg,
    ai_assistant_language_model_metrics,
    commits_prs,
    ai_compare_acceptance,
    ai_compare_usage_user_level,
)
from generators.utils import load_story

CATALOG = "playground_prod"

with open("config/entities.yaml") as f:
    entities = yaml.safe_load(f)
story = load_story("narrative")

# Scope to the demo-acme-direct org (orgs[1]) — same as direct/insert.py
entities_direct = {**entities, "orgs": [entities["orgs"][1]]}

# ── Part 1: consumption-layer + base data ─────────────────────────────────────
# Order:
#   1. license_info / user_engagement / language_agg / language_model_metrics —
#      new tables, all 3 tools per row.
#   2. commits_prs — new table, ai_tool array tagging.
#   3. ai_compare_acceptance + ai_compare_usage_user_level — ADDITIVE rows
#      for cursor + claude code into the same tables that direct/ writes
#      copilot rows into.

statements = []
statements += [(ai_assistant_license_info.TABLE, s)
               for s in ai_assistant_license_info.generate(CATALOG, entities_direct, story)]
statements += [(ai_assistant_user_engagement.TABLE, s)
               for s in ai_assistant_user_engagement.generate(CATALOG, entities_direct, story)]
statements += [(ai_assistant_programming_language_agg.TABLE, s)
               for s in ai_assistant_programming_language_agg.generate(CATALOG, entities_direct, story)]
statements += [(ai_assistant_language_model_metrics.TABLE, s)
               for s in ai_assistant_language_model_metrics.generate(CATALOG, entities_direct, story)]
statements += [(commits_prs.TABLE, s)
               for s in commits_prs.generate(CATALOG, entities_direct, story)]
statements += [(ai_compare_acceptance.TABLE + " (non-copilot)", s)
               for s in ai_compare_acceptance.generate(CATALOG, entities_direct, story)]
statements += [(ai_compare_usage_user_level.TABLE + " (non-copilot)", s)
               for s in ai_compare_usage_user_level.generate(CATALOG, entities_direct, story)]

for i, (table, sql) in enumerate(statements, 1):
    print(f"[{i}/{len(statements)}] {table}...", end=" ")
    spark.sql(sql)
    print("done")

# ── Part 2: filter_values_unity — surface each AI tool to the dashboard ──────
# The AI Code Comparison charts populate their tool-picker dropdown by reading
# DISTINCT tool_type from v_filter_group_values_kpi_flattened_unity filtered
# by the dashboard's filter_group + KPI UUID. We attach 3 rows (one per tool)
# to the existing Acme filter_group_id (created by dora/insert.py).

AI_CODE_COMPARISON_KPIS = [
    "ai_code_comparison_banner",
    "ai_code_comparison_combined_throughput",
    "ai_code_comparison_distinct_tool",
    "ai_code_comparison_hours_saved_per_week_org",
    "ai_code_comparison_infrastructure_cost",
    "ai_code_comparison_languages_matrix",
    "ai_code_comparison_leaderboard",
    "ai_code_comparison_license_allocated_vs_active_users",
    "ai_code_comparison_matrix",
    "ai_code_comparison_model_usage_org",
    "ai_code_comparison_radar_chart_metrics",
    "ai_code_comparison_tool_adoption_gap",
    "ai_code_comparison_tool_adoption_rate",
    "ai_code_comparison_tool_comparison_matrix",
    "ai_code_comparison_tool_performance_score",
]
KPIS_SQL = ", ".join(f"'{k}'" for k in AI_CODE_COMPARISON_KPIS)

# Find the Acme filter_group_id created by dora/insert.py (created_by tag).
fg_row = spark.sql(f"""
    SELECT filter_group_id FROM {CATALOG}.master_data.filter_groups_unity
    WHERE createdBy = 'seed-data@demo.io' AND level_3 = 'demo-acme-corp'
    LIMIT 1
""").collect()

if not fg_row:
    raise RuntimeError(
        "No Acme filter_group found — run notebooks/dora/insert.py first so "
        "the Acme filter_group_id exists in master_data.filter_groups_unity."
    )
FILTER_GROUP_ID = fg_row[0]["filter_group_id"]
print(f"Attaching ai-tool filter rows to filter_group_id={FILTER_GROUP_ID}")

CREATED_BY = "seed-data-ai-compare@demo.io"  # distinct tag for clean delete


# NOTE on filter_name: the view v_filter_group_values_kpi_flattened_unity
# pivots filter_name → typed array columns (project_url, org_name, etc.). The
# raw tool_type column is preserved as-is. AI Code Comparison's license_info
# chart joins strictly on f.org_name = s.access_level_name (no 'x' escape
# hatch like DORA), so we MUST surface a non-null org_name in the view. We
# do that by inserting rows with filter_name='org_name', filter_values=[org]
# and the per-tool tool_type set on the raw column. One row per tool ends up
# with (tool_type=<tool>, org_name=[demo-acme-direct]) in the view, which
# satisfies both strict and forgiving join shapes.

def _fvu(tool_type, sort_number):
    _id = str(uuid.uuid4())
    org = entities_direct["orgs"][0]["name"]
    spark.sql(f"""
        INSERT INTO {CATALOG}.master_data.filter_values_unity
            (id, filter_group_id, tool_type, filter_name, filter_values, kpi_uuids,
             custom_fieldName, created_by, created_at, updated_by, updated_at,
             source, active, sort_number)
        VALUES (
            '{_id}', '{FILTER_GROUP_ID}',
            '{tool_type}', 'org_name',
            array('{org}'),
            array({KPIS_SQL}),
            'null', '{CREATED_BY}', CURRENT_TIMESTAMP(),
            '{CREATED_BY}', CURRENT_TIMESTAMP(),
            'user', true, {sort_number}
        )
    """)


for idx, tool in enumerate(entities.get("ai_tools", [])):
    _fvu(tool["name"], 100 + idx)
print(f"filter_values_unity: inserted {len(entities.get('ai_tools', []))} ai-tool rows "
      f"(filter_name='org_name', org={entities_direct['orgs'][0]['name']!r})")
