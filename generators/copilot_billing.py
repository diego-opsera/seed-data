"""
Generator for source_to_stage.raw_github_copilot_billing.

One row per (org, week) — feeds v_github_copilot_seats_billing via a JOIN
with master_data.github_copilot_orgs_mapping.

Columns:
  org_name, seat_breakdown_total, seat_breakdown_active_this_cycle,
  seat_breakdown_inactive_this_cycle, seat_breakdown_added_this_cycle,
  seat_breakdown_pending_invitation, seat_breakdown_pending_cancellation,
  plan_type, public_code_suggestions, ide_chat, platform_chat, cli,
  record_insert_datetime

seat_breakdown_total  = allocated seats (quarterly step-function matching seats_usage)
active_this_cycle     = number of active seats for that week
inactive              = total - active
"""
from datetime import date
from .utils import date_range, active_user_count, expand_users, _sql_val

TABLE  = "raw_github_copilot_billing"
SCHEMA = "source_to_stage"

INSERT_SQL = """\
INSERT INTO {catalog}.source_to_stage.raw_github_copilot_billing
  (org_name, seat_breakdown_total, seat_breakdown_active_this_cycle,
   seat_breakdown_inactive_this_cycle, seat_breakdown_added_this_cycle,
   seat_breakdown_pending_invitation, seat_breakdown_pending_cancellation,
   plan_type, public_code_suggestions, ide_chat, platform_chat, cli,
   record_insert_datetime)
VALUES
{values};"""

# Must match seats_usage._QUARTER_ALLOC
_QUARTER_ALLOC = [
    (date(2026, 1, 1), 150),
    (date(2025, 10, 1), 100),
    (date(2025, 7, 1),   67),
    (date(2025, 4, 1),   45),
    (date(2025, 1, 1),   45),
]


def _allocated_count(d: date) -> int:
    for q_start, count in _QUARTER_ALLOC:
        if d >= q_start:
            return count
    return _QUARTER_ALLOC[-1][1]


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]   # demo-acme-direct (entities_direct has orgs[1] remapped to [0])
    all_users = expand_users(entities, story)

    mondays = [
        d for d in date_range(story["start_date"], story["end_date"])
        if d.weekday() == 0
    ]

    value_lines = []
    for d in mondays:
        allocated_n = _allocated_count(d)
        active_n    = min(active_user_count(d, story, len(all_users)), allocated_n)
        inactive_n  = allocated_n - active_n
        snap_ts     = f"{d.isoformat()} 12:00:00"

        value_lines.append(
            f"  ({_sql_val(org_name)}, "
            f"{allocated_n}, {active_n}, {inactive_n}, 0, 0, 0, "
            f"'enterprise', 'disabled', 'enabled', 'enabled', 'enabled', "
            f"TIMESTAMP '{snap_ts}')"
        )

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
