"""
Generator for source_to_stage.raw_github_copilot_seats.

One row per (seat, week) — feeds v_github_copilot_seats_usage_user_level
via a JOIN with master_data.github_copilot_orgs_mapping.

Allocated seats follow a quarterly step-function (administrative pattern),
growing 1.5x each quarter. Active users within that allocation are driven
by the story's user-count ramp (human behaviour). Extra allocated-but-inactive
seats have last_activity_at = NULL so the dashboard can distinguish the two.

Scoped to org_name = demo-acme-direct for safe delete.
"""
from datetime import date
from .utils import date_range, expand_users, active_user_count, _sql_val

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

# Quarterly allocated-seat step-function.
# Working backwards from Q1 2026 = 150 (clearly above peak usage ~100).
# Each quarter = previous * 1.5, rounded.
_QUARTER_ALLOC = [
    (date(2026, 1, 1), 150),   # Q1 2026
    (date(2025, 10, 1), 100),  # Q4 2025
    (date(2025, 7, 1),   67),  # Q3 2025
    (date(2025, 4, 1),   45),  # Q2 2025
    (date(2025, 1, 1),   45),  # Q1 2025 (story starts mid-quarter)
]


def _allocated_count(d: date) -> int:
    """Return the number of allocated seats for a given date (step function)."""
    for q_start, count in _QUARTER_ALLOC:
        if d >= q_start:
            return count
    return _QUARTER_ALLOC[-1][1]


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][1]["name"]          # demo-acme-direct
    all_users = expand_users(entities, story)
    start     = date.fromisoformat(story["start_date"])

    created_at = f"{start.isoformat()} 00:00:00"

    mondays = [
        d for d in date_range(story["start_date"], story["end_date"])
        if d.weekday() == 0
    ]

    value_lines = []
    for d in mondays:
        allocated_n = _allocated_count(d)
        active_n    = active_user_count(d, story, len(all_users))
        if allocated_n == 0:
            continue

        snap_ts = f"{d.isoformat()} 12:00:00"

        # Active seats: last_activity_at = snapshot time
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

        # Allocated-but-inactive seats: last_activity_at = NULL
        for i in range(active_n, allocated_n):
            if i < len(all_users):
                login    = all_users[i]["login"]
                user_id  = str(all_users[i]["id"])
                team_name = all_users[i].get("team", "demo-backend")
            else:
                login    = f"demo-seat-{i + 1}"
                user_id  = str(900000 + i)
                team_name = "demo-backend"
            team_id = abs(hash(team_name)) % 90000 + 10000
            value_lines.append(
                f"  ({_sql_val(org_name)}, {_sql_val(login)}, "
                f"{_sql_val(login)}, {_sql_val(user_id)}, "
                f"{team_id}, {_sql_val(team_name)}, {_sql_val(team_name)}, "
                f"NULL, NULL, "
                f"TIMESTAMP '{created_at}', TIMESTAMP '{snap_ts}', NULL, "
                f"TIMESTAMP '{snap_ts}', 'enterprise', NULL)"
            )

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
