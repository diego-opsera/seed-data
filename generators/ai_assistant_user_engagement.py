"""
Generator for consumption_layer.ai_assistant_user_engagement.
One row per (tool, day) at organization level — drives the Chat & IDE
Active User Engagement chart on the AI Code Comparison dashboard.

active users = tool_active_users(d) (allocation_on × active_share × day_scale)
chat / ide split is controlled by tool['chat_share'] (rest = IDE).
Chat-heavy tools (Claude Code) show higher chat_active_users; IDE-heavy
tools (Copilot) show the inverse.

Deletion scoped to level_name = 'demo-acme-direct'.
"""
from .utils import (
    date_range, jitter, _sql_val, active_user_count, expand_users,
    tool_is_live, tool_allocation_on, tool_active_users,
)

TABLE  = "ai_assistant_user_engagement"
SCHEMA = "consumption_layer"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.ai_assistant_user_engagement
  (ai_assistant_tool_name, usage_date, level_type, level_name,
   users, chat_active_users, ide_active_users)
VALUES
{values};"""


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]
    tools     = entities.get("ai_tools", [])
    if not tools:
        return []
    noise     = max(5, story.get("noise_pct", 10))
    all_users = expand_users(entities, story)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        baseline = active_user_count(d, story, len(all_users))
        if baseline == 0:
            continue

        for tool in tools:
            if not tool_is_live(tool, d, story):
                continue
            active = tool_active_users(tool, d, story, baseline)
            if active == 0:
                continue

            chat_share = float(tool.get("chat_share", 0.35))
            chat_n = jitter(round(active * chat_share), noise,
                            hash((str(d), tool["name"], "chat")) % (2 ** 31))
            chat_n = max(0, min(active, chat_n))
            ide_n  = active - chat_n  # primary mode split — must sum to `active`

            usage_ts = f"TIMESTAMP '{d.isoformat()} 00:00:00'"
            value_lines.append(
                f"  ({_sql_val(tool['name'])}, {usage_ts}, 'organization', "
                f"{_sql_val(org_name)}, {active}, {chat_n}, {ide_n})"
            )

    if not value_lines:
        return []
    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
