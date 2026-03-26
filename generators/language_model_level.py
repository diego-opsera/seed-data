"""
Generator for enterprise_user_language_model_level_copilot_metrics.
One row per (usage_date, user, language, model). No nested arrays.
"""
from datetime import date
from .utils import date_range, jitter, acceptance_subset, validate_row, _sql_val, trend_base, day_scale, expand_users, active_user_count, LANG_ACCEPTANCE_RATES


TABLE = "enterprise_user_language_model_level_copilot_metrics"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.{table}
  (usage_date, enterprise_id, enterprise, user_id, user_login, assignee_login,
   language, model,
   accepted_loc_sum, generated_loc_sum,
   code_acceptance_activity_count, code_generation_activity_count,
   loc_added_sum, loc_deleted_sum, loc_suggested_to_add_sum, loc_suggested_to_delete_sum)
VALUES
{values};"""


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    ent = entities["enterprise"]
    noise = story.get("noise_pct", 0)
    languages = entities["languages"]
    models = entities["models"]
    all_users = expand_users(entities, story)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        n = active_user_count(d, story, len(all_users))
        if n == 0:
            continue
        scale = day_scale(d, story)
        base = trend_base(story, d)
        scaled = {k: max(0, round(v * scale)) for k, v in base.items()}
        for i, user in enumerate(all_users[:n]):
            language = user["language"]
            model = models[i % len(models)]
            acc_rate = LANG_ACCEPTANCE_RATES.get(language, 0.45)
            seed = hash((str(d), user["id"], language, model)) % 100000

            code_gen = jitter(scaled["code_generation_activity_count"], noise, seed)
            code_acc = acceptance_subset(code_gen, acc_rate)
            loc_sugg_add = jitter(scaled["loc_suggested_to_add"], noise, seed + 1)
            loc_sugg_del = jitter(scaled["loc_suggested_to_delete"], noise, seed + 2)
            loc_add = acceptance_subset(loc_sugg_add, acc_rate)
            loc_del = acceptance_subset(loc_sugg_del, acc_rate)

            row = {
                "code_generation_activity_count": code_gen,
                "code_acceptance_activity_count": code_acc,
                "loc_suggested_to_add_sum": loc_sugg_add,
                "loc_suggested_to_delete_sum": loc_sugg_del,
                "loc_added_sum": loc_add,
                "loc_deleted_sum": loc_del,
            }
            validate_row(row, TABLE)

            value_lines.append(
                f"  ({_sql_val(d)}, {ent['id']}, {_sql_val(ent['name'])}, "
                f"{user['id']}, {_sql_val(user['login'])}, {_sql_val(user['assignee_login'])}, "
                f"{_sql_val(language)}, {_sql_val(model)}, "
                f"{loc_add}, {loc_sugg_add}, "
                f"{code_acc}, {code_gen}, "
                f"{loc_add}, {loc_del}, {loc_sugg_add}, {loc_sugg_del})"
            )

    return [INSERT_SQL.format(catalog=catalog, table=TABLE, values=",\n".join(value_lines))]
