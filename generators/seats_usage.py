"""
Generator for v_github_copilot_seats_usage_user_level.
One row per (business day, user) for all allocated seat holders.
Active users receive copilot_usage_date = that day; over-allocated seats get NULL.
Drives: License Trend, License Table (allocated vs active licenses).
"""
from datetime import date
from .utils import date_range, expand_users, active_user_count, lerp, _sql_val


TABLE = "v_github_copilot_seats_usage_user_level"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.{table}
  (record_insert_datetime, cleansed_assignee_login, copilot_usage_date,
   copilot_usage_datetime, org_name, org_assignee_login, enterprise_id)
VALUES
{values};"""

# Seats provisioned slightly ahead of the active user trend (~12% buffer).
# This produces a realistic "inactive licenses" slice in the dashboard.
_ALLOC_BUFFER = 1.12


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    ent = entities["enterprise"]
    org_name = entities["orgs"][0]["name"]
    all_users = expand_users(entities, story)
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])
    total_days = max((end - start).days, 1)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        if d.weekday() >= 5:  # license snapshots are taken on business days only
            continue
        active_n = active_user_count(d, story, len(all_users))
        t = max(0.0, min(1.0, (d - start).days / total_days))
        allocated_n = min(
            len(all_users),
            max(active_n, round(lerp(
                story["user_count_start"] * _ALLOC_BUFFER,
                story["user_count_end"] * _ALLOC_BUFFER,
                t,
            ))),
        )

        snap_ts = f"{d.isoformat()} 12:00:00"
        for i, user in enumerate(all_users[:allocated_n]):
            if i < active_n:
                usage_date_sql = _sql_val(d)
                usage_ts_sql   = f"TIMESTAMP '{d.isoformat()} 09:00:00'"
            else:
                usage_date_sql = "NULL"
                usage_ts_sql   = "NULL"
            value_lines.append(
                f"  (TIMESTAMP '{snap_ts}', {_sql_val(user['login'])}, "
                f"{usage_date_sql}, {usage_ts_sql}, "
                f"{_sql_val(org_name)}, {_sql_val(user['assignee_login'])}, "
                f"{ent['id']})"
            )

    return [INSERT_SQL.format(catalog=catalog, table=TABLE, values=",\n".join(value_lines))]
