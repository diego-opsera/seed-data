"""
Generator for enterprise_user_feature_level_copilot_metrics.
One row per (usage_date, user, feature).
No nested arrays — simplest table to populate.
"""
from datetime import date
from .utils import date_range, jitter, acceptance_subset, validate_row, _sql_val, trend_base, day_scale, expand_users, active_user_count


TABLE = "enterprise_user_feature_level_copilot_metrics"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.{table}
  (usage_date, enterprise_id, enterprise, user_id, user_login, assignee_login,
   feature, user_initiated_interaction_count, accepted_loc_sum, generated_loc_sum,
   code_acceptance_activity_count, code_generation_activity_count,
   loc_added_sum, loc_deleted_sum, loc_suggested_to_add_sum, loc_suggested_to_delete_sum)
VALUES
{values};"""


def _row_values(
    usage_date: date,
    enterprise_id: int,
    enterprise: str,
    user_id: int,
    user_login: str,
    assignee_login: str,
    feature: str,
    base: dict,
    noise_pct: float,
    seed: int,
) -> tuple[dict, str]:
    # Split base values across this feature (single feature = full value)
    code_gen = jitter(base["code_generation_activity_count"], noise_pct, seed)
    code_acc = acceptance_subset(code_gen, 0.45)
    loc_sugg_add = jitter(base["loc_suggested_to_add"], noise_pct, seed + 1)
    loc_sugg_del = jitter(base["loc_suggested_to_delete"], noise_pct, seed + 2)
    loc_add = acceptance_subset(loc_sugg_add, 0.45)
    loc_del = acceptance_subset(loc_sugg_del, 0.45)
    interactions = jitter(base["user_initiated_interaction_count"], noise_pct, seed + 3)

    row = {
        "code_generation_activity_count": code_gen,
        "code_acceptance_activity_count": code_acc,
        "loc_suggested_to_add_sum": loc_sugg_add,
        "loc_suggested_to_delete_sum": loc_sugg_del,
        "loc_added_sum": loc_add,
        "loc_deleted_sum": loc_del,
    }
    validate_row(row, TABLE)

    sql = (
        f"  ({_sql_val(usage_date)}, {enterprise_id}, {_sql_val(enterprise)}, "
        f"{user_id}, {_sql_val(user_login)}, {_sql_val(assignee_login)}, "
        f"{_sql_val(feature)}, {interactions}, {loc_add}, {loc_sugg_add}, "
        f"{code_acc}, {code_gen}, "
        f"{loc_add}, {loc_del}, {loc_sugg_add}, {loc_sugg_del})"
    )
    return row, sql


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    """
    Returns a list of complete INSERT SQL statements, one per batch of rows.
    """
    ent = entities["enterprise"]
    noise = story.get("noise_pct", 0)
    active_features = story["active_features"]
    all_users = expand_users(entities, story)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        n = active_user_count(d, story, len(all_users))
        if n == 0:
            continue
        scale = day_scale(d, story)
        base = trend_base(story, d)
        scaled = {k: max(0, round(v * scale)) for k, v in base.items()}
        for user in all_users[:n]:
            for i, feature in enumerate(active_features):
                seed = hash((str(d), user["id"], feature)) % 100000
                _, sql = _row_values(
                    usage_date=d,
                    enterprise_id=ent["id"],
                    enterprise=ent["name"],
                    user_id=user["id"],
                    user_login=user["login"],
                    assignee_login=user["assignee_login"],
                    feature=feature,
                    base=scaled,
                    noise_pct=noise,
                    seed=seed,
                )
                value_lines.append(sql)

    # Batch into single INSERT (all rows at once for Phase 1 scale)
    statement = INSERT_SQL.format(
        catalog=catalog,
        table=TABLE,
        values=",\n".join(value_lines),
    )
    return [statement]
