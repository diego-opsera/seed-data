"""
Generator for source_to_stage.raw_github_copilot_seats.

One row per (user, week) for active weeks — feeds
v_github_copilot_seats_usage_user_level via a JOIN with
master_data.github_copilot_orgs_mapping.

Scoped to org_name = demo-acme-direct for safe delete.
"""
from datetime import date, datetime, timedelta
from .utils import date_range, expand_users, active_user_count, lerp, _sql_val

TABLE  = "raw_github_copilot_seats"
SCHEMA = "source_to_stage"

INSERT_SQL = """\
INSERT INTO {catalog}.source_to_stage.raw_github_copilot_seats
  (org_name, org_assignee_login, assignee_login, assignee_id,
   assigning_team_id, assigning_team_name, assigning_team_slug,
   last_activity_at, last_activity_editor,
   created_at, updated_at, pending_cancellation_date,
   source_record_insert_datetime, plan_type, message)
VALUES
{values};"""


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][1]["name"]          # demo-acme-direct
    all_users = expand_users(entities, story)
    start     = date.fromisoformat(story["start_date"])
    end       = date.fromisoformat(story["end_date"])

    # Seat creation date = start of story for all users
    created_at = f"{start.isoformat()} 00:00:00"

    # Weekly snapshots: iterate Mondays only
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
            team_id   = abs(hash(team_name)) % 90000 + 10000

            value_lines.append(
                f"  ({_sql_val(org_name)}, {_sql_val(user['login'])}, "
                f"{_sql_val(user['login'])}, {_sql_val(str(user['id']))}, "
                f"{team_id}, {_sql_val(team_name)}, {_sql_val(team_name)}, "
                f"TIMESTAMP '{snap_ts}', {_sql_val(user.get('ide', 'vscode'))}, "
                f"TIMESTAMP '{created_at}', TIMESTAMP '{snap_ts}', NULL, "
                f"TIMESTAMP '{snap_ts}', 'enterprise', NULL)"
            )

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
