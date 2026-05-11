"""
Generator for consumption_layer.ai_assistant_programming_language_agg.
One row per (tool, language, week) at organization level — drives the
Programming Language Support chart on the AI Code Comparison dashboard.

Weekly cadence (Mondays only) — language adoption changes slowly so daily
rows would be noise. `engaged_users` per (tool, language) is derived from
the per-tool active count × the language weight share for that tool.

Note: queries reference `level_type_name` for the org-name column (vs
`level_name` on other tables). We match that exact name here.

Deletion scoped to level_type_name = 'demo-acme-direct'.
"""
import random
from datetime import date
from .utils import (
    date_range, jitter, _sql_val, expand_users, active_user_count,
    tool_is_live, tool_active_users, _LANG_WEIGHTS,
)

TABLE  = "ai_assistant_programming_language_agg"
SCHEMA = "consumption_layer"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.ai_assistant_programming_language_agg
  (tool_name, language, level_type, level_type_name, usage_date, engaged_users)
VALUES
{values};"""


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]
    tools     = entities.get("ai_tools", [])
    if not tools:
        return []
    noise     = max(5, story.get("noise_pct", 10))
    languages = entities["languages"]
    all_users = expand_users(entities, story)

    # Distinct active users per language under this story, computed once.
    lang_counts: dict[str, int] = {l: 0 for l in languages}
    for u in all_users:
        if u["language"] in lang_counts:
            lang_counts[u["language"]] += 1
    total = sum(lang_counts.values()) or 1
    lang_share = {l: lang_counts[l] / total for l in languages}

    mondays = [
        d for d in date_range(story["start_date"], story["end_date"])
        if d.weekday() == 0
    ]

    value_lines = []
    for d in mondays:
        baseline = active_user_count(d, story, len(all_users))
        if baseline == 0:
            continue
        for tool in tools:
            if not tool_is_live(tool, d, story):
                continue
            tool_active = tool_active_users(tool, d, story, baseline)
            if tool_active == 0:
                continue

            for lang in languages:
                share = lang_share.get(lang, 0)
                base_engaged = round(tool_active * share)
                if base_engaged == 0:
                    continue
                engaged = jitter(base_engaged, noise,
                                 hash((str(d), tool["name"], lang)) % (2 ** 31))
                engaged = max(0, min(tool_active, engaged))
                if engaged == 0:
                    continue
                usage_ts = f"TIMESTAMP '{d.isoformat()} 00:00:00'"
                value_lines.append(
                    f"  ({_sql_val(tool['name'])}, {_sql_val(lang)}, "
                    f"'organization', {_sql_val(org_name)}, "
                    f"{usage_ts}, {engaged})"
                )

    if not value_lines:
        return []
    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
