"""
Generator for v_github_copilot_metrics_ide_org_level.
One row per (date, org, language, ide) for each active (language, ide) combination.
Aggregated metrics are derived by grouping active users on that day by their
stable (language, ide) assignment — consistent with the user-level generators.
Drives: Acceptance Rate charts, Developer Impact table.
"""
from collections import defaultdict
from datetime import date
from .utils import (
    date_range, jitter, acceptance_subset, expand_users, active_user_count,
    day_scale, trend_base, lerp, LANG_ACCEPTANCE_RATES, _sql_val,
)


TABLE = "v_github_copilot_metrics_ide_org_level"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.{table}
  (copilot_usage_date, org_name, enterprise_id,
   ide_code_completion_editor_language, ide_code_completion_editor_name,
   ide_code_completion_code_suggestions, ide_code_completion_code_acceptances,
   ide_code_completion_code_lines_suggested, ide_code_completion_code_lines_accepted,
   ide_code_completion_code_lines_suggested_to_add, ide_code_completion_code_lines_suggested_to_delete,
   ide_code_completion_code_lines_accepted_to_add, ide_code_completion_code_lines_accepted_to_delete,
   ide_code_completion_engaged_users,
   ide_chat_engaged_users, agent_engaged_users,
   total_active_users, total_engaged_users,
   ide_chat_model_name, ide_chat_editor_name, ide_chat_editor_chats,
   total_interactions_count,
   agent_lines_accepted_to_add, agent_lines_accepted_to_delete)
VALUES
{values};"""

# Fraction of code-completion users that also engage chat / agent each day.
_CHAT_FRAC  = (0.40, 0.75)
_AGENT_FRAC = (0.05, 0.40)


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    ent = entities["enterprise"]
    org_name = entities["orgs"][0]["name"]
    noise = story.get("noise_pct", 0)
    all_users = expand_users(entities, story)
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])
    total_days = max((end - start).days, 1)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        active_n = active_user_count(d, story, len(all_users))
        if active_n == 0:
            continue
        scale  = day_scale(d, story)
        base   = trend_base(story, d)
        scaled = {k: max(0, round(v * scale)) for k, v in base.items()}
        t = max(0.0, min(1.0, (d - start).days / total_days))
        chat_frac  = lerp(_CHAT_FRAC[0],  _CHAT_FRAC[1],  t)
        agent_frac = lerp(_AGENT_FRAC[0], _AGENT_FRAC[1], t)
        total_active = active_n

        # Group active users by their stable (language, ide) pair
        groups: dict[tuple, list] = defaultdict(list)
        for user in all_users[:active_n]:
            groups[(user["language"], user["ide"])].append(user)

        for (lang, ide), users in groups.items():
            n_users    = len(users)
            lang_rate  = LANG_ACCEPTANCE_RATES.get(lang, 0.45)

            suggestions = sum(
                jitter(scaled["code_generation_activity_count"], noise,
                       hash((str(d), u["id"], "sugg_org")) % (2 ** 31))
                for u in users
            )
            acceptances = acceptance_subset(suggestions, lang_rate)

            lines_sugg_add = sum(
                jitter(scaled["loc_suggested_to_add"], noise,
                       hash((str(d), u["id"], "lsa_org")) % (2 ** 31))
                for u in users
            )
            lines_sugg_del = sum(
                jitter(scaled["loc_suggested_to_delete"], noise,
                       hash((str(d), u["id"], "lsd_org")) % (2 ** 31))
                for u in users
            )
            lines_sugg_total = lines_sugg_add + lines_sugg_del
            lines_acc_add    = acceptance_subset(lines_sugg_add, lang_rate)
            lines_acc_del    = acceptance_subset(lines_sugg_del, lang_rate)
            lines_acc_total  = lines_acc_add + lines_acc_del

            interactions   = sum(
                jitter(scaled["user_initiated_interaction_count"], noise,
                       hash((str(d), u["id"], "int_org")) % (2 ** 31))
                for u in users
            )
            chat_users     = max(0, round(n_users * chat_frac))
            agent_users    = max(0, round(n_users * agent_frac))
            agent_acc_add  = acceptance_subset(lines_acc_add, 0.20)
            agent_acc_del  = acceptance_subset(lines_acc_del, 0.20)

            value_lines.append(
                f"  ({_sql_val(d)}, {_sql_val(org_name)}, {ent['id']}, "
                f"{_sql_val(lang)}, {_sql_val(ide)}, "
                f"{suggestions}, {acceptances}, "
                f"{lines_sugg_total}, {lines_acc_total}, "
                f"{lines_sugg_add}, {lines_sugg_del}, "
                f"{lines_acc_add}, {lines_acc_del}, "
                f"{n_users}, "
                f"{chat_users}, {agent_users}, "
                f"{total_active}, {total_active}, "
                f"'gpt-4o', {_sql_val(ide)}, {interactions}, "
                f"{interactions}, "
                f"{agent_acc_add}, {agent_acc_del})"
            )

    return [INSERT_SQL.format(catalog=catalog, table=TABLE, values=",\n".join(value_lines))]
