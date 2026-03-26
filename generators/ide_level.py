"""
Generator for enterprise_user_ide_level_copilot_metrics.
One row per (usage_date, user, ide).
Has one STRUCT column (last_known_plugin_version) but no ARRAY columns.
"""
from datetime import date
from .utils import date_range, jitter, acceptance_subset, validate_row, _sql_val, named_struct, PLUGIN_NAMES, PLUGIN_VERSIONS, trend_base, day_scale, expand_users, active_user_count


TABLE = "enterprise_user_ide_level_copilot_metrics"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.{table}
  (usage_date, enterprise_id, enterprise, user_id, user_login, assignee_login,
   ide, user_initiated_interaction_count, accepted_loc_sum, generated_loc_sum,
   code_acceptance_activity_count, code_generation_activity_count,
   loc_added_sum, loc_deleted_sum, loc_suggested_to_add_sum, loc_suggested_to_delete_sum,
   last_known_plugin_version)
VALUES
{values};"""


def _plugin_struct(ide: str, sampled_at: str) -> str:
    return named_struct({
        "plugin": PLUGIN_NAMES.get(ide, "copilot"),
        "plugin_version": PLUGIN_VERSIONS.get(ide, "1.0.0"),
        "sampled_at": sampled_at,
    })


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    ent = entities["enterprise"]
    noise = story.get("noise_pct", 0)
    all_users = expand_users(entities, story)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        n = active_user_count(d, story, len(all_users))
        if n == 0:
            continue
        scale = day_scale(d, story)
        base = trend_base(story, d)
        scaled = {k: max(0, round(v * scale)) for k, v in base.items()}
        sampled_at = f"{d}T00:00:00Z"
        for i, user in enumerate(all_users[:n]):
            ide = user["ide"]
            seed = hash((str(d), user["id"], ide)) % 100000

            code_gen = jitter(scaled["code_generation_activity_count"], noise, seed)
            code_acc = acceptance_subset(code_gen, 0.45)
            loc_sugg_add = jitter(scaled["loc_suggested_to_add"], noise, seed + 1)
            loc_sugg_del = jitter(scaled["loc_suggested_to_delete"], noise, seed + 2)
            loc_add = acceptance_subset(loc_sugg_add, 0.45)
            loc_del = acceptance_subset(loc_sugg_del, 0.45)
            interactions = jitter(scaled["user_initiated_interaction_count"], noise, seed + 3)

            row = {
                "code_generation_activity_count": code_gen,
                "code_acceptance_activity_count": code_acc,
                "loc_suggested_to_add_sum": loc_sugg_add,
                "loc_suggested_to_delete_sum": loc_sugg_del,
                "loc_added_sum": loc_add,
                "loc_deleted_sum": loc_del,
            }
            validate_row(row, TABLE)

            plugin = _plugin_struct(ide, sampled_at)

            sql = (
                f"  ({_sql_val(d)}, {ent['id']}, {_sql_val(ent['name'])}, "
                f"{user['id']}, {_sql_val(user['login'])}, {_sql_val(user['assignee_login'])}, "
                f"{_sql_val(ide)}, {interactions}, {loc_add}, {loc_sugg_add}, "
                f"{code_acc}, {code_gen}, "
                f"{loc_add}, {loc_del}, {loc_sugg_add}, {loc_sugg_del}, "
                f"{plugin})"
            )
            value_lines.append(sql)

    statement = INSERT_SQL.format(
        catalog=catalog,
        table=TABLE,
        values=",\n".join(value_lines),
    )
    return [statement]
