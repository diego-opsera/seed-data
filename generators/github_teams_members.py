"""
Generator for source_to_stage.raw_github_teams_members.
Seeds team membership for demo-acme-direct users so they appear in
v_github_teams_members_current and pass the team_name IS NOT NULL
filter in copilot_heavy_user_percentage.sql.

CRITICAL: record_update_datetime must equal the current MAX in the table
(2025-10-24 03:37:54.966189) so our rows are included by the view's
  WHERE record_update_datetime IN (SELECT MAX(...) ...)
filter WITHOUT pushing other orgs' rows out of scope.

Deletion scoped to org_name = 'demo-acme-direct'.
"""
from .utils import expand_users, _sql_val

TABLE  = "raw_github_teams_members"
SCHEMA = "source_to_stage"

# Must match MAX(record_update_datetime) in the live table.
# Checked 2026-04-01: max = 2025-10-24 03:37:54.966189
_MAX_UPDATE_TS = "2025-10-24 03:37:54.966189"

INSERT_SQL = """\
INSERT INTO {catalog}.source_to_stage.raw_github_teams_members
  (assignee_login, assignee_login_id,
   team_name, team_slug, team_members_url,
   org_name, record_inserted_by, record_insert_datetime, record_update_datetime,
   message)
VALUES
{values};"""


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]   # demo-acme-direct
    all_users = expand_users(entities, story)

    now_ts = _MAX_UPDATE_TS

    value_lines = []
    for user in all_users:
        team      = user.get("team", "demo-backend")
        team_url  = f"https://api.github.com/orgs/{org_name}/teams/{team}/members"
        value_lines.append(
            f"  ({_sql_val(user['login'])}, {_sql_val(str(user['id']))}, "
            f"{_sql_val(team)}, {_sql_val(team)}, {_sql_val(team_url)}, "
            f"{_sql_val(org_name)}, 'seed-data', "
            f"TIMESTAMP '{now_ts}', TIMESTAMP '{now_ts}', "
            f"NULL)"
        )

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
