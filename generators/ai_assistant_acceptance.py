"""
Generator for consumption_layer.ai_assistant_acceptance_info.
One org-level row per day — drives "hours saved/month" on the Executive Summary.

Mirrors the same story arc as ide_org_level.py but aggregated to a single
org-level row per day (level='organization', level_name='demo-acme-direct').

Deletion scoped to level_name = 'demo-acme-direct'.
"""
from collections import defaultdict
from datetime import date
from .utils import (
    date_range, jitter, acceptance_subset, expand_users, active_user_count,
    day_scale, trend_base, lerp, LANG_ACCEPTANCE_RATES, _sql_val,
)

TABLE  = "ai_assistant_acceptance_info"
SCHEMA = "consumption_layer"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.ai_assistant_acceptance_info
  (ai_assistant_tool_name, ai_assistant_usage_date, level, level_name, parent_name,
   ide_total_acceptances, ide_total_suggestions,
   ide_total_lines_accepted, ide_total_lines_suggested)
VALUES
{values};"""

_CHAT_FRAC = (0.40, 0.75)


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]
    noise     = story.get("noise_pct", 0)
    all_users = expand_users(entities, story)
    start     = date.fromisoformat(story["start_date"])
    end       = date.fromisoformat(story["end_date"])
    total_days = max((end - start).days, 1)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        active_n = active_user_count(d, story, len(all_users))
        if active_n == 0:
            continue

        scale  = day_scale(d, story)
        base   = trend_base(story, d)
        scaled = {k: max(0, round(v * scale)) for k, v in base.items()}

        groups: dict[tuple, list] = defaultdict(list)
        for user in all_users[:active_n]:
            groups[(user["language"], user["ide"])].append(user)

        day_suggestions = 0
        day_acceptances = 0
        day_lines_sugg  = 0
        day_lines_acc   = 0

        for (lang, ide), users in groups.items():
            lang_rate = LANG_ACCEPTANCE_RATES.get(lang, 0.45)
            for u in users:
                sugg = jitter(scaled["code_generation_activity_count"], noise,
                              hash((str(d), u["id"], "sugg_org")) % (2 ** 31))
                acc  = acceptance_subset(sugg, lang_rate)
                ls   = jitter(scaled["loc_suggested_to_add"], noise,
                              hash((str(d), u["id"], "lsa_org")) % (2 ** 31))
                la   = acceptance_subset(ls, lang_rate)
                day_suggestions += sugg
                day_acceptances += acc
                day_lines_sugg  += ls
                day_lines_acc   += la

        usage_ts = f"TIMESTAMP '{d.isoformat()} 00:00:00'"
        value_lines.append(
            f"  ('github copilot', {usage_ts}, 'organization', "
            f"{_sql_val(org_name)}, NULL, "
            f"{float(day_acceptances)}, {float(day_suggestions)}, "
            f"{float(day_lines_acc)}, {float(day_lines_sugg)})"
        )

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
