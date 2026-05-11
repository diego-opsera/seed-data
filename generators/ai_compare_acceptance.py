"""
Additive generator for consumption_layer.ai_assistant_acceptance_info.
Emits org-level rows for every ai_tool EXCEPT 'github copilot' (which is
owned by ai_assistant_acceptance.py via direct/insert.py).

Used by notebooks/ai_compare/insert.py to add cursor + claude code rows
alongside the copilot rows direct/ already wrote. The AI Code Comparison
dashboard reads all 3 tools from the same table.

Per-tool counts are scaled from the global trend_base by:
  - tool_allocation_on(d)        — license ramp during rollout
  - tool['active_share']         — fraction of allocated that are active
  - tool['productivity_mult']    — per-active-user lines-accepted multiplier

Deletion: by ai_assistant_tool_name IN (<non-copilot tools>) so direct/'s
copilot rows are untouched. Done in notebooks/ai_compare/delete.py.
"""
from collections import defaultdict
from .utils import (
    date_range, jitter, acceptance_subset, expand_users, active_user_count,
    day_scale, trend_base, LANG_ACCEPTANCE_RATES, _sql_val,
    incident_multiplier, tool_is_live, tool_allocation_on,
)

TABLE  = "ai_assistant_acceptance_info"
SCHEMA = "consumption_layer"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.ai_assistant_acceptance_info
  (ai_assistant_tool_name, ai_assistant_usage_date, level, level_name, parent_name,
   ide_total_acceptances, ide_total_suggestions,
   ide_total_lines_accepted, ide_total_lines_suggested)
VALUES
{values};"""


def _non_copilot_tools(entities: dict) -> list[dict]:
    return [t for t in entities.get("ai_tools", [])
            if t["name"].lower() != "github copilot"]


def tool_names(entities: dict) -> list[str]:
    """Names of the tools this generator owns — used by delete.py scoping."""
    return [t["name"] for t in _non_copilot_tools(entities)]


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]
    tools     = _non_copilot_tools(entities)
    if not tools:
        return []
    noise     = story.get("noise_pct", 0)
    all_users = expand_users(entities, story)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        baseline = active_user_count(d, story, len(all_users))
        if baseline == 0:
            continue
        inc_mult = incident_multiplier(d)
        baseline = max(0, min(len(all_users), round(baseline * inc_mult)))
        if baseline == 0:
            continue

        scale  = day_scale(d, story)
        base   = trend_base(story, d)
        scaled = {k: max(0, round(v * scale)) for k, v in base.items()}

        for tool in tools:
            if not tool_is_live(tool, d, story):
                continue
            allocated = tool_allocation_on(tool, d, story)
            if allocated == 0:
                continue
            active_n = max(0, round(min(allocated, baseline) * float(tool["active_share"])))
            if active_n == 0:
                continue

            mult = float(tool.get("productivity_mult", 1.0))
            tool_users = all_users[:active_n]

            groups: dict[tuple, list] = defaultdict(list)
            for user in tool_users:
                groups[(user["language"], user["ide"])].append(user)

            day_suggestions = 0
            day_acceptances = 0
            day_lines_sugg  = 0
            day_lines_acc   = 0

            for (lang, _ide), users in groups.items():
                lang_rate = LANG_ACCEPTANCE_RATES.get(lang, 0.45)
                for u in users:
                    sugg = jitter(scaled["code_generation_activity_count"], noise,
                                  hash((str(d), u["id"], tool["name"], "sugg")) % (2 ** 31))
                    acc  = acceptance_subset(sugg, lang_rate)
                    ls   = jitter(scaled["loc_suggested_to_add"], noise,
                                  hash((str(d), u["id"], tool["name"], "lsa")) % (2 ** 31))
                    la   = acceptance_subset(round(ls * mult), lang_rate)
                    ls   = max(ls, la)  # suggestions must be >= acceptances
                    day_suggestions += sugg
                    day_acceptances += acc
                    day_lines_sugg  += ls
                    day_lines_acc   += la

            usage_ts = f"TIMESTAMP '{d.isoformat()} 00:00:00'"
            value_lines.append(
                f"  ({_sql_val(tool['name'])}, {usage_ts}, 'organization', "
                f"{_sql_val(org_name)}, NULL, "
                f"{float(day_acceptances)}, {float(day_suggestions)}, "
                f"{float(day_lines_acc)}, {float(day_lines_sugg)})"
            )

    if not value_lines:
        return []
    batch_size = 500
    statements = []
    for i in range(0, len(value_lines), batch_size):
        batch = value_lines[i:i + batch_size]
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(batch)))
    return statements
