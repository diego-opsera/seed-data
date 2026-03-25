"""
Generator for enterprise_user_language_model_level_copilot_metrics.
One row per (usage_date, user, language, model). No nested arrays.
"""
from datetime import date
from .utils import date_range, jitter, acceptance_subset, validate_row, _sql_val


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
    base = story["per_user_per_day"]
    noise = story.get("noise_pct", 0)

    languages = entities["languages"]
    models = entities["models"]
    # Each user gets one primary language and one primary model for simplicity
    # Rotate assignments across users so there's variety
    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        for i, user in enumerate(entities["users"]):
            language = languages[i % len(languages)]
            model = models[i % len(models)]
            seed = hash((str(d), user["id"], language, model)) % 100000

            code_gen = jitter(base["code_generation_activity_count"], noise, seed)
            code_acc = acceptance_subset(code_gen, 0.45)
            loc_sugg_add = jitter(base["loc_suggested_to_add"], noise, seed + 1)
            loc_sugg_del = jitter(base["loc_suggested_to_delete"], noise, seed + 2)
            loc_add = acceptance_subset(loc_sugg_add, 0.45)
            loc_del = acceptance_subset(loc_sugg_del, 0.45)

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
