"""
source_to_stage.raw_github_teams_members — team membership for Team Split.

Without these rows every developer in individuals[] falls back to
team = 'Engineering' and the Team Split view has nothing to group by.

THE TRAP (BUGS.md #2): base_datasets.v_github_teams_members_current filters

    where a.record_update_datetime in
        (select max(record_update_datetime) from source_to_stage.raw_github_teams_members)

a single global MAX across the whole table, with no per-org scoping. So:

  * set record_update_datetime HIGHER than the current max and every other
    org disappears from the view — including opsera-it-networking's 5,190 rows
    and demo-acme-direct's 100. OpseraEngineering is already hidden this way:
    its max is 03:36:58 against the global 03:37:54.
  * set it LOWER and our own rows never appear.

It has to match the existing global max EXACTLY, which is why the value is not
hard-coded here — insert.py resolves it at runtime with
`SELECT MAX(record_update_datetime) FROM ...` and passes it in. Hard-coding
would silently break the moment the upstream ETL writes a newer batch.
"""
from .sqlutil import json_lit, sq, ts

TABLE = "source_to_stage.raw_github_teams_members"
COLUMNS = ("assignee_login, assignee_login_id, team_name, team_slug, "
           "team_members_url, org_name, record_inserted_by, "
           "record_insert_datetime, record_update_datetime, message")
INSERTED_BY = "seed-data-vf"


def generate(catalog: str, entities: dict, story: dict, *,
             record_update_datetime: str | None = None, **_) -> list[str]:
    if not record_update_datetime:
        raise ValueError(
            "record_update_datetime is required and must equal the CURRENT global "
            "MAX(record_update_datetime) of source_to_stage.raw_github_teams_members. "
            "v_github_teams_members_current filters on that single global max with no "
            "org scoping, so a higher value hides every other org and a lower value "
            "hides ours (BUGS.md #2). insert.py resolves it at runtime."
        )

    from .story import roster

    org = entities["orgs"][0]["name"]
    users = roster(entities, story)

    values = []
    for u in users:
        team = u["team"]
        payload = {
            "login": u["login"],
            "id": str(u["id"]),
            "node_id": f"U_{u['id']}",
            "html_url": f"https://github.com/{u['login']}",
            "url": f"https://api.github.com/users/{u['login']}",
            "type": "User",
            "site_admin": False,
        }
        values.append(
            f"  ({sq(u['login'])}, {sq(str(u['id']))}, {sq(team)}, {sq(team)}, "
            f"{sq(f'https://api.github.com/orgs/{org}/teams/{team}/members')}, "
            f"{sq(org)}, {sq(INSERTED_BY)}, {ts(record_update_datetime)}, "
            f"{ts(record_update_datetime)}, {json_lit(payload)})"
        )

    return [f"INSERT INTO {catalog}.{TABLE}\n  ({COLUMNS})\nVALUES\n" + ",\n".join(values) + ";"]


def delete_sql(catalog: str, entities: dict) -> list[str]:
    org = entities["orgs"][0]["name"]
    return [
        f"DELETE FROM {catalog}.{TABLE} "
        f"WHERE org_name = '{org}' AND record_inserted_by = '{INSERTED_BY}';"
    ]
