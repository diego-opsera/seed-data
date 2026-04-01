"""
Generator for master_data.github_copilot_orgs_mapping.

One row for demo-acme-direct — required so the JOIN in
v_github_copilot_seats_usage_user_level resolves correctly.

Scoped by org_name = 'demo-acme-direct' for safe delete.
"""
from .utils import _sql_val

TABLE  = "github_copilot_orgs_mapping"
SCHEMA = "master_data"

INSERT_SQL = """\
INSERT INTO {catalog}.master_data.github_copilot_orgs_mapping
  (org_name, linked_org_name, master_license_org_name, unity_org_name,
   record_inserted_by, record_insert_datetime)
VALUES
{values};"""


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][1]["name"]   # demo-acme-direct

    row = (
        f"  ({_sql_val(org_name)}, "
        f"array({_sql_val(org_name)}), "
        f"{_sql_val(org_name)}, "
        f"{_sql_val(org_name)}, "
        f"'seed-data', "
        f"TIMESTAMP '2025-01-01 00:00:00')"
    )

    return [INSERT_SQL.format(catalog=catalog, values=row)]
