"""
Generator for consumption_layer.ai_assistant_language_model_metrics.
One row per (tool, model, language, week) at organization level — drives:
  - Languages Matrix
  - Tool Comparison Matrix
  - Radar Chart Metrics
on the AI Code Comparison dashboard.

For each tool, engaged users + lines are split across its primary_models
(first model gets the largest slice — captures real "default-model" usage).
Lines-accepted scales by productivity_mult so Cursor/Claude-Code show
higher per-user output than Copilot at the same engaged_users count.

Deletion scoped to level_name = 'demo-acme-direct'.
"""
from .utils import (
    date_range, jitter, _sql_val, expand_users, active_user_count,
    tool_is_live, tool_active_users, tool_allocation_on,
    LANG_ACCEPTANCE_RATES, trend_base, day_scale,
)

TABLE  = "ai_assistant_language_model_metrics"
SCHEMA = "consumption_layer"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.ai_assistant_language_model_metrics
  (tool_name, model_name, language, usage_date, level_type, level_name,
   engaged_users, total_lines_suggested, total_lines_accepted, total_allocated_licenses)
VALUES
{values};"""


# Model weight schedule — first model in `primary_models` carries ~55%, second
# ~30%, third ~15%. Tools with one model get 100%.
_MODEL_WEIGHTS = [0.55, 0.30, 0.15]


def _model_split(models: list[str]) -> list[tuple[str, float]]:
    if not models:
        return []
    n = min(len(models), len(_MODEL_WEIGHTS))
    weights = _MODEL_WEIGHTS[:n]
    s = sum(weights)
    return [(models[i], weights[i] / s) for i in range(n)]


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]
    tools     = entities.get("ai_tools", [])
    if not tools:
        return []
    noise     = max(5, story.get("noise_pct", 10))
    languages = entities["languages"]
    all_users = expand_users(entities, story)

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
        base = trend_base(story, d)  # used for per-user lines suggested base

        for tool in tools:
            if not tool_is_live(tool, d, story):
                continue
            tool_active = tool_active_users(tool, d, story, baseline)
            if tool_active == 0:
                continue
            allocated = tool_allocation_on(tool, d, story)
            mult = float(tool.get("productivity_mult", 1.0))
            models = _model_split(tool.get("primary_models", []))
            if not models:
                continue

            for lang in languages:
                share = lang_share.get(lang, 0)
                lang_users = round(tool_active * share)
                if lang_users == 0:
                    continue
                lang_rate = LANG_ACCEPTANCE_RATES.get(lang, 0.45)

                # Weekly per-user productivity ≈ 5 weekdays * daily base
                per_user_sugg_week = base.get("loc_suggested_to_add", 100) * 5

                for model, weight in models:
                    eu = round(lang_users * weight)
                    if eu == 0:
                        continue
                    sugg = jitter(round(eu * per_user_sugg_week), noise,
                                  hash((str(d), tool["name"], model, lang, "sugg")) % (2 ** 31))
                    acc  = round(sugg * lang_rate * mult)
                    acc  = min(sugg, acc)

                    usage_ts = f"TIMESTAMP '{d.isoformat()} 00:00:00'"
                    value_lines.append(
                        f"  ({_sql_val(tool['name'])}, {_sql_val(model)}, "
                        f"{_sql_val(lang)}, {usage_ts}, "
                        f"'organization', {_sql_val(org_name)}, "
                        f"{eu}, {sugg}, {acc}, {allocated})"
                    )

    if not value_lines:
        return []
    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
