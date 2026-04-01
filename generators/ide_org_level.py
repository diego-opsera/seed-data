"""
Generator for base_datasets.github_copilot_metrics_ide_org_level.

One row per (date, org, language, ide) for each active combination.
Drives Acceptance Rate charts and Developer Impact table via
v_github_copilot_metrics_ide_org_level (thin view over this table).

Scoped to org_name = demo-acme-direct for safe delete.
"""
from collections import defaultdict
from datetime import date, datetime
from .utils import (
    date_range, jitter, acceptance_subset, expand_users, active_user_count,
    day_scale, trend_base, lerp, LANG_ACCEPTANCE_RATES, _sql_val,
)

TABLE  = "github_copilot_metrics_ide_org_level"
SCHEMA = "base_datasets"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.github_copilot_metrics_ide_org_level
  (copilot_usage_date, org_name,
   total_active_users, total_engaged_users,
   ide_chat_engaged_users, ide_chat_model_name, ide_chat_custom_model_flag,
   ide_chat_editor_name, ide_chat_editor_chats,
   ide_chat_editor_engaged_users,
   ide_chat_editor_chat_copy_events, ide_chat_editor_chat_insertion_events,
   ide_code_completion_engaged_users,
   ide_code_completion_model_name, ide_code_completion_editor_name,
   ide_code_completion_editor_language,
   ide_code_completion_laguage_engaged_users,
   ide_code_completion_code_suggestions, ide_code_completion_code_acceptances,
   ide_code_completion_code_lines_suggested, ide_code_completion_code_lines_accepted,
   total_suggestions_count, total_acceptances_count,
   total_lines_suggested, total_lines_accepted,
   total_active_users_usage,
   total_chat_acceptances, total_chat_turns, total_active_chat_users,
   rest_api_name, record_insert_datetime, record_inserted_by)
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

    now_ts = "TIMESTAMP '2025-01-01 00:00:00'"

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        active_n = active_user_count(d, story, len(all_users))
        if active_n == 0:
            continue
        scale  = day_scale(d, story)
        base   = trend_base(story, d)
        scaled = {k: max(0, round(v * scale)) for k, v in base.items()}
        t = max(0.0, min(1.0, (d - start).days / total_days))
        chat_frac = lerp(_CHAT_FRAC[0], _CHAT_FRAC[1], t)

        total_active = active_n

        # Group active users by stable (language, ide) pair
        groups: dict[tuple, list] = defaultdict(list)
        for user in all_users[:active_n]:
            groups[(user["language"], user["ide"])].append(user)

        for (lang, ide), users in groups.items():
            n_users   = len(users)
            lang_rate = LANG_ACCEPTANCE_RATES.get(lang, 0.45)

            suggestions = sum(
                jitter(scaled["code_generation_activity_count"], noise,
                       hash((str(d), u["id"], "sugg_org")) % (2 ** 31))
                for u in users
            )
            acceptances = acceptance_subset(suggestions, lang_rate)

            lines_sugg = sum(
                jitter(scaled["loc_suggested_to_add"], noise,
                       hash((str(d), u["id"], "lsa_org")) % (2 ** 31))
                for u in users
            )
            lines_acc = acceptance_subset(lines_sugg, lang_rate)

            chat_users   = max(0, round(n_users * chat_frac))
            chat_chats   = sum(
                jitter(scaled["user_initiated_interaction_count"], noise,
                       hash((str(d), u["id"], "chat_org")) % (2 ** 31))
                for u in users
            )
            chat_accepts = acceptance_subset(chat_chats, 0.35)
            copy_events  = acceptance_subset(chat_chats, 0.20)
            insert_events = acceptance_subset(chat_chats, 0.15)

            value_lines.append(
                f"  ({_sql_val(d)}, {_sql_val(org_name)}, "
                f"{total_active}, {total_active}, "
                f"{chat_users}, 'gpt-4o', 0, "
                f"{_sql_val(ide)}, {chat_chats}, "
                f"{chat_users}, "
                f"{copy_events}, {insert_events}, "
                f"{n_users}, "
                f"'default', {_sql_val(ide)}, "
                f"{_sql_val(lang)}, "
                f"{n_users}, "
                f"{suggestions}, {acceptances}, "
                f"{lines_sugg}, {lines_acc}, "
                f"{suggestions}, {acceptances}, "
                f"{lines_sugg}, {lines_acc}, "
                f"{total_active}, "
                f"{chat_accepts}, {chat_chats}, {chat_users}, "
                f"'REST', {now_ts}, 'seed-data')"
            )

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
