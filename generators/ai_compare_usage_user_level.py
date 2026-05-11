"""
Additive generator for consumption_layer.ai_code_assistant_usage_user_level.
Emits per-user weekly snapshot rows for every ai_tool EXCEPT 'github copilot'
(owned by ai_usage_user_level.py via direct/insert.py).

Used by notebooks/ai_compare/insert.py to add cursor + claude code rows
alongside the copilot rows direct/ already wrote.

User → tool assignment:
  - cursor:        deterministic subset of size tool.allocation
  - claude code:   deterministic subset of size tool.allocation
  Overlap is allowed — a power-user can show up on multiple tools.

Activity is gated by tool rollout (tool_is_live(d)) so each tool's
last_activity_date series starts at its rollout, not at story start.

Deletion: by ai_tool_name IN (<non-copilot tools>) so direct/'s copilot
rows are untouched. Done in notebooks/ai_compare/delete.py.
"""
import random
from .utils import (
    date_range, expand_users, active_user_count, _sql_val,
    tool_is_live, tool_allocation_on, assign_users_to_tool,
)

TABLE  = "ai_code_assistant_usage_user_level"
SCHEMA = "consumption_layer"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.ai_code_assistant_usage_user_level
  (ai_tool_name, last_activity_datetime, last_activity_date, last_activity_hour,
   org_name, assignee_login_email, assigning_team_id, assigning_team_name,
   org_assignee_login, cleansed_org_assignee_login,
   assignee_id, assignee_login, cleansed_assignee_login,
   record_insert_datetime)
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
    all_users = expand_users(entities, story)

    # Stable per-tool roster — same set every run.
    tool_rosters = {t["name"]: assign_users_to_tool(all_users, t, seed=42)
                    for t in tools}

    mondays = [
        d for d in date_range(story["start_date"], story["end_date"])
        if d.weekday() == 0
    ]

    value_lines = []
    for d in mondays:
        baseline_active = active_user_count(d, story, len(all_users))
        if baseline_active == 0:
            continue

        snap_ts = f"{d.isoformat()} 12:00:00"

        for tool in tools:
            if not tool_is_live(tool, d, story):
                continue
            allocated = tool_allocation_on(tool, d, story)
            if allocated == 0:
                continue

            roster = tool_rosters[tool["name"]]
            cap = min(allocated, baseline_active, len(roster))
            active_n = max(0, round(cap * float(tool["active_share"])))
            if active_n == 0:
                continue

            rng = random.Random(hash((str(d), tool["name"])) % (2 ** 31))
            active_users = rng.sample(roster, active_n)

            for user in active_users:
                team_name = user.get("team", "demo-backend")
                team_id   = str(abs(hash(team_name)) % 90000 + 10000)
                email     = f"{user['login']}@demo-acme-direct.com"
                value_lines.append(
                    f"  ({_sql_val(tool['name'])}, "
                    f"TIMESTAMP '{snap_ts}', DATE '{d.isoformat()}', 12, "
                    f"{_sql_val(org_name)}, {_sql_val(email)}, "
                    f"{_sql_val(team_id)}, {_sql_val(team_name)}, "
                    f"{_sql_val(user['login'])}, {_sql_val(user['login'])}, "
                    f"{_sql_val(str(user['id']))}, {_sql_val(user['login'])}, {_sql_val(user['login'])}, "
                    f"TIMESTAMP '{snap_ts}')"
                )

    if not value_lines:
        return []
    batch_size = 500
    statements = []
    for i in range(0, len(value_lines), batch_size):
        batch = value_lines[i:i + batch_size]
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(batch)))
    return statements
