"""
master_data.github_copilot_orgs_mapping — one row for demo-acme-vf.

Small but load-bearing: base_datasets.v_github_teams_members_current JOINS this
table and explodes linked_org_name, so without a mapping row our team members
never surface and every developer falls back to team = 'Engineering'.

    from source_to_stage.raw_github_teams_members a
    join master_data.github_copilot_orgs_mapping b on a.org_name = b.org_name
    LATERAL VIEW explode(linked_org_name) lv as org_name
    where a.record_update_datetime in (select max(...) from raw_github_teams_members)

demo-acme-direct already has an equivalent row inserted by 'seed-data'.
"""
from .sqlutil import sq, ts

TABLE = "master_data.github_copilot_orgs_mapping"
COLUMNS = ("org_name, linked_org_name, master_license_org_name, unity_org_name, "
           "record_inserted_by, record_insert_datetime")
INSERTED_BY = "seed-data-vf"


def generate(catalog: str, entities: dict, story: dict, **_) -> list[str]:
    org = entities["orgs"][0]["name"]
    return [
        f"INSERT INTO {catalog}.{TABLE}\n  ({COLUMNS})\nVALUES\n"
        f"  ({sq(org)}, ARRAY({sq(org)}), {sq(org)}, {sq(org)}, "
        f"{sq(INSERTED_BY)}, {ts('2025-01-01 00:00:00')});"
    ]


def delete_sql(catalog: str, entities: dict) -> list[str]:
    org = entities["orgs"][0]["name"]
    return [
        f"DELETE FROM {catalog}.{TABLE} "
        f"WHERE org_name = '{org}' AND record_inserted_by = '{INSERTED_BY}';"
    ]
