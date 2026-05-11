"""
Generator for consumption_layer.ai_assistant_license_info.
One row per (tool, day) at organization level — drives:
  - Adoption Rate (license_usage / allocated)
  - Adoption Gap
  - License Allocated vs Active Users
on the AI Code Comparison dashboard.

License count ramps from 0 over a 30-day rollout starting at the tool's
rollout_offset_days, then plateaus at tool['allocation']. Active-in-cycle
follows the same curve scaled by tool['active_share'] with mild jitter.

Deletion scoped to access_level_name = 'demo-acme-direct'.
"""
from .utils import (
    date_range, jitter, _sql_val,
    tool_allocation_on, tool_is_live,
)

TABLE  = "ai_assistant_license_info"
SCHEMA = "consumption_layer"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.ai_assistant_license_info
  (ai_assistant_tool_name, ai_assistant_usage_date, access_level, access_level_name,
   allocated_licenses, licenses_active_in_cycle)
VALUES
{values};"""


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    tools    = entities.get("ai_tools", [])
    if not tools:
        return []
    noise = max(5, story.get("noise_pct", 10))

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        for tool in tools:
            if not tool_is_live(tool, d, story):
                continue
            allocated = tool_allocation_on(tool, d, story)
            if allocated == 0:
                continue
            base_active = round(allocated * float(tool["active_share"]))
            active = jitter(base_active, noise,
                            hash((str(d), tool["name"], "active")) % (2 ** 31))
            active = min(active, allocated)

            usage_ts = f"TIMESTAMP '{d.isoformat()} 00:00:00'"
            value_lines.append(
                f"  ({_sql_val(tool['name'])}, {usage_ts}, 'organization', "
                f"{_sql_val(org_name)}, {allocated}, {active})"
            )

    if not value_lines:
        return []
    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
