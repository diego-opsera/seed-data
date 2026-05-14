"""
Generator for base_datasets.github_copilot_developer_usage_org_level.

Long-format / EAV-style table the Developer Language And Editor Usage
dashboard reads from. Each row is keyed by (date, org_name, param_name,
parameter), with param_name in {programmingLanguage, editor, ide_model,
chat_model}.

Why this is a separate generator:
  The dashboard's `all_names` CTE pulls DISTINCT parameter values across
  the entire table with no org filter, so other tenants' data pollutes the
  dropdown. Our `source` CTE is filtered to demo-acme-direct, so until we
  contribute rows here our values are all 0 even for languages we actually
  use. See notebooks/core/debug_dev_lang_editor.py for the trace.

Per-day budget is split across languages by _LANG_WEIGHTS and across
editors by _IDE_WEIGHTS. ide_model and chat_model both go to a single
'GitHub Copilot Open AI' bucket (the canonical name from real data).

Deletion scoped to org_name = 'demo-acme-direct'.
"""
from .utils import (
    date_range, jitter, acceptance_subset, expand_users, active_user_count,
    day_scale, trend_base, LANG_ACCEPTANCE_RATES, _sql_val,
    incident_multiplier, _LANG_WEIGHTS, _IDE_WEIGHTS,
)

TABLE  = "github_copilot_developer_usage_org_level"
SCHEMA = "base_datasets"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.github_copilot_developer_usage_org_level
  (copilot_usage_date, org_name, param_name, parameter,
   total_lines_accepted, total_lines_suggested, total_agent_lines)
VALUES
{values};"""

# The real table uses 'jetbrains' (not 'intellij'). Map our entity value to
# what the dashboard's all_names CTE expects.
_EDITOR_NAME_MAP = {
    "intellij": "jetbrains",
}

# Default model name for both code completion and chat — matches the
# canonical entry already present in the table.
_DEFAULT_MODEL_NAME = "GitHub Copilot Open AI"


def _weighted_split(total: int, weights: dict) -> dict:
    """Split `total` across keys proportional to weights. Sums exactly."""
    if total <= 0 or not weights:
        return {k: 0 for k in weights}
    s = sum(weights.values()) or 1
    raw = {k: total * w / s for k, w in weights.items()}
    out = {k: int(v) for k, v in raw.items()}
    remainder = total - sum(out.values())
    # Distribute remainder to keys with the largest fractional part
    fracs = sorted(weights.keys(), key=lambda k: raw[k] - out[k], reverse=True)
    for k in fracs[:remainder]:
        out[k] += 1
    return out


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]
    noise     = story.get("noise_pct", 0)
    all_users = expand_users(entities, story)
    languages = entities["languages"]
    ides      = entities["ides"]

    # Editor weights keyed by the dashboard-canonical name (map intellij→jetbrains)
    editor_weights = {
        _EDITOR_NAME_MAP.get(ide, ide): _IDE_WEIGHTS.get(ide, 5)
        for ide in ides
    }
    lang_weights = {l: _LANG_WEIGHTS.get(l, 10) for l in languages}

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        active_n = active_user_count(d, story, len(all_users))
        if active_n == 0:
            continue
        inc_mult = incident_multiplier(d)
        active_n = max(0, min(len(all_users), round(active_n * inc_mult)))
        if active_n == 0:
            continue

        scale = day_scale(d, story)
        base  = trend_base(story, d)
        per_user_sugg = max(0, round(base["loc_suggested_to_add"] * scale))
        per_user_gen  = max(0, round(base["code_generation_activity_count"] * scale))

        # Org-day totals (lines)
        seed = hash((str(d), "dev_usage")) % (2 ** 31)
        day_sugg = jitter(per_user_sugg * active_n, noise, seed)
        # Use the weighted-average language acceptance rate as the org-wide rate
        avg_acc_rate = (
            sum(LANG_ACCEPTANCE_RATES.get(l, 0.45) * lang_weights[l] for l in languages)
            / max(sum(lang_weights.values()), 1)
        )
        day_acc = acceptance_subset(day_sugg, avg_acc_rate)

        # 1. programmingLanguage rows — split across our languages by weight
        sugg_by_lang = _weighted_split(day_sugg, lang_weights)
        for lang in languages:
            ls = sugg_by_lang[lang]
            la = acceptance_subset(ls, LANG_ACCEPTANCE_RATES.get(lang, 0.45))
            value_lines.append(
                f"  (DATE '{d.isoformat()}', {_sql_val(org_name)}, "
                f"'programmingLanguage', {_sql_val(lang)}, "
                f"{la}, {ls}, NULL)"
            )

        # 2. editor rows — split across IDEs by weight, using canonical names
        sugg_by_editor = _weighted_split(day_sugg, editor_weights)
        for editor_name, _w in editor_weights.items():
            ls = sugg_by_editor[editor_name]
            la = acceptance_subset(ls, avg_acc_rate)
            value_lines.append(
                f"  (DATE '{d.isoformat()}', {_sql_val(org_name)}, "
                f"'editor', {_sql_val(editor_name)}, "
                f"{la}, {ls}, NULL)"
            )

        # 3. ide_model — single 'GitHub Copilot Open AI' bucket gets the lot
        value_lines.append(
            f"  (DATE '{d.isoformat()}', {_sql_val(org_name)}, "
            f"'ide_model', {_sql_val(_DEFAULT_MODEL_NAME)}, "
            f"{day_acc}, {day_sugg}, NULL)"
        )

        # 4. chat_model — same bucket, narrower volume (~30% of completion)
        chat_sugg = max(0, round(day_sugg * 0.30))
        chat_acc  = acceptance_subset(chat_sugg, 0.35)
        value_lines.append(
            f"  (DATE '{d.isoformat()}', {_sql_val(org_name)}, "
            f"'chat_model', {_sql_val(_DEFAULT_MODEL_NAME)}, "
            f"{chat_acc}, {chat_sugg}, NULL)"
        )

    if not value_lines:
        return []
    batch_size = 500
    statements = []
    for i in range(0, len(value_lines), batch_size):
        batch = value_lines[i:i + batch_size]
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(batch)))
    return statements
