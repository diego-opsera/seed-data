"""
Generator for consumption_layer.ai_code_assistant_usage_user_level.
One row per (user, week) — drives "Monthly savings" active user count
on the Executive Summary.

Weekly Monday snapshots mirroring seats_usage, scoped to demo-acme-direct.

Deletion scoped to org_name = 'demo-acme-direct'.
"""
from datetime import date
from .utils import date_range, expand_users, active_user_count, _sql_val

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


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]
    all_users = expand_users(entities, story)
    start     = date.fromisoformat(story["start_date"])

    mondays = [
        d for d in date_range(story["start_date"], story["end_date"])
        if d.weekday() == 0
    ]

    value_lines = []
    for d in mondays:
        active_n = active_user_count(d, story, len(all_users))
        if active_n == 0:
            continue

        snap_ts = f"{d.isoformat()} 12:00:00"

        for user in all_users[:active_n]:
            team_name = user.get("team", "demo-backend")
            team_id   = str(abs(hash(team_name)) % 90000 + 10000)
            email     = f"{user['login']}@demo-acme-direct.com"
            value_lines.append(
                f"  ('github copilot', "
                f"TIMESTAMP '{snap_ts}', DATE '{d.isoformat()}', 12, "
                f"{_sql_val(org_name)}, {_sql_val(email)}, "
                f"{_sql_val(team_id)}, {_sql_val(team_name)}, "
                f"{_sql_val(user['login'])}, {_sql_val(user['login'])}, "
                f"{_sql_val(str(user['id']))}, {_sql_val(user['login'])}, {_sql_val(user['login'])}, "
                f"TIMESTAMP '{snap_ts}')"
            )

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
