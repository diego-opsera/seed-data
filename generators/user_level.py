"""
Generator for enterprise_user_level_copilot_metrics.
One row per (usage_date, user) with five nested ARRAY<STRUCT> columns.
"""
from datetime import date
from .utils import (
    date_range, jitter, acceptance_subset, split_across, validate_row, _sql_val,
    sql_array, trend_base, day_scale, expand_users, active_user_count,
    LANG_ACCEPTANCE_RATES,
    totals_by_feature_entry,
    totals_by_ide_entry,
    totals_by_language_feature_entry,
    totals_by_language_model_entry,
    totals_by_model_feature_entry,
)


TABLE = "enterprise_user_level_copilot_metrics"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.{table}
  (usage_date, enterprise_id, enterprise, user_id, user_login, assignee_login,
   user_initiated_interaction_count, code_generation_activity_count, code_acceptance_activity_count,
   used_agent, used_chat,
   loc_suggested_to_add_sum, loc_suggested_to_delete_sum, loc_deleted_sum, loc_added_sum,
   totals_by_ide, totals_by_feature, totals_by_language_feature,
   totals_by_language_model, totals_by_model_feature)
VALUES
{values};"""


def _build_arrays(
    ide: str,
    active_features: list[str],
    language: str,
    model: str,
    code_gen: int,
    code_acc: int,
    interactions: int,
    accepted_loc: int,
    generated_loc: int,
    sampled_at: str,
) -> tuple[str, str, str, str, str]:
    """Build all five nested array literals for one user-day row."""

    # totals_by_ide: single entry for this user's primary IDE
    ide_entry = totals_by_ide_entry(
        ide=ide,
        code_gen=code_gen,
        code_accept=code_acc,
        interaction_count=interactions,
        accepted_loc=accepted_loc,
        generated_loc=generated_loc,
        sampled_at=sampled_at,
    )

    # totals_by_feature: split activity across active features
    feature_code_gens = split_across(code_gen, len(active_features))
    feature_code_accs = split_across(code_acc, len(active_features))
    feature_interactions = split_across(interactions, len(active_features))
    feature_accepted = split_across(accepted_loc, len(active_features))
    feature_generated = split_across(generated_loc, len(active_features))
    feature_entries = [
        totals_by_feature_entry(
            feature=f,
            code_gen=feature_code_gens[i],
            code_accept=feature_code_accs[i],
            interaction_count=feature_interactions[i],
            accepted_loc=feature_accepted[i],
            generated_loc=feature_generated[i],
        )
        for i, f in enumerate(active_features)
    ]

    # totals_by_language_feature: one entry per feature for this user's language
    lang_feature_entries = [
        totals_by_language_feature_entry(
            language=language,
            feature=f,
            code_gen=feature_code_gens[i],
            code_accept=feature_code_accs[i],
            accepted_loc=feature_accepted[i],
            generated_loc=feature_generated[i],
        )
        for i, f in enumerate(active_features)
    ]

    # totals_by_language_model: single entry for this user's language+model
    lang_model_entry = totals_by_language_model_entry(
        language=language,
        model=model,
        code_gen=code_gen,
        code_accept=code_acc,
        accepted_loc=accepted_loc,
        generated_loc=generated_loc,
    )

    # totals_by_model_feature: one entry per feature for this user's model
    model_feature_entries = [
        totals_by_model_feature_entry(
            model=model,
            feature=f,
            code_gen=feature_code_gens[i],
            code_accept=feature_code_accs[i],
            interaction_count=feature_interactions[i],
            accepted_loc=feature_accepted[i],
            generated_loc=feature_generated[i],
        )
        for i, f in enumerate(active_features)
    ]

    return (
        sql_array([ide_entry]),
        sql_array(feature_entries),
        sql_array(lang_feature_entries),
        sql_array([lang_model_entry]),
        sql_array(model_feature_entries),
    )


def _compute_values(d: date, user: dict, i: int, entities: dict, story: dict) -> dict | None:
    """
    Compute scalar values for one user-day.
    Returns None only for vacation periods (scale == 0.0).
    """
    scale = day_scale(d, story)
    if scale == 0.0:
        return None

    base = trend_base(story, d)
    scaled = {k: max(0, round(v * scale)) for k, v in base.items()}
    noise = story.get("noise_pct", 0)
    seed = hash((str(d), user["id"])) % 100000

    models = entities["models"]
    acc_rate = LANG_ACCEPTANCE_RATES.get(user["language"], 0.45)

    code_gen = jitter(scaled["code_generation_activity_count"], noise, seed)
    code_acc = acceptance_subset(code_gen, acc_rate)
    loc_sugg_add = jitter(scaled["loc_suggested_to_add"], noise, seed + 1)
    loc_sugg_del = jitter(scaled["loc_suggested_to_delete"], noise, seed + 2)
    loc_add = acceptance_subset(loc_sugg_add, acc_rate)
    loc_del = acceptance_subset(loc_sugg_del, acc_rate)
    interactions = jitter(scaled["user_initiated_interaction_count"], noise, seed + 3)

    return {
        "usage_date": str(d),
        "user_id": user["id"],
        "ide": user["ide"],
        "language": user["language"],
        "model": models[i % len(models)],
        "code_generation_activity_count": code_gen,
        "code_acceptance_activity_count": code_acc,
        "loc_suggested_to_add_sum": loc_sugg_add,
        "loc_suggested_to_delete_sum": loc_sugg_del,
        "loc_added_sum": loc_add,
        "loc_deleted_sum": loc_del,
        "user_initiated_interaction_count": interactions,
        "used_chat": interactions > 0,
    }


def build_user_row_dicts(entities: dict, story: dict) -> list[dict]:
    """
    Build plain dicts for all active user-days.
    Used by enterprise_level.generate() to aggregate totals consistently.
    """
    all_users = expand_users(entities, story)
    rows = []
    for d in date_range(story["start_date"], story["end_date"]):
        n = active_user_count(d, story, len(all_users))
        for i, user in enumerate(all_users[:n]):
            row = _compute_values(d, user, i, entities, story)
            if row is not None:
                rows.append(row)
    return rows


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    ent = entities["enterprise"]
    active_features = story["active_features"]
    all_users = expand_users(entities, story)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        n = active_user_count(d, story, len(all_users))
        if n == 0:
            continue
        sampled_at = f"{d}T00:00:00.000Z"
        for i, user in enumerate(all_users[:n]):
            vals = _compute_values(d, user, i, entities, story)
            if vals is None:
                continue

            validate_row(vals, TABLE)
            used_agent = "FALSE"
            used_chat = "TRUE" if vals["used_chat"] else "FALSE"

            arr_ide, arr_feat, arr_lang_feat, arr_lang_model, arr_model_feat = _build_arrays(
                ide=vals["ide"],
                active_features=active_features,
                language=vals["language"],
                model=vals["model"],
                code_gen=vals["code_generation_activity_count"],
                code_acc=vals["code_acceptance_activity_count"],
                interactions=vals["user_initiated_interaction_count"],
                accepted_loc=vals["loc_added_sum"],
                generated_loc=vals["loc_suggested_to_add_sum"],
                sampled_at=sampled_at,
            )

            value_lines.append(
                f"  ({_sql_val(d)}, {ent['id']}, {_sql_val(ent['name'])}, "
                f"{user['id']}, {_sql_val(user['login'])}, {_sql_val(user['assignee_login'])}, "
                f"{vals['user_initiated_interaction_count']}, {vals['code_generation_activity_count']}, {vals['code_acceptance_activity_count']}, "
                f"{used_agent}, {used_chat}, "
                f"{vals['loc_suggested_to_add_sum']}, {vals['loc_suggested_to_delete_sum']}, {vals['loc_deleted_sum']}, {vals['loc_added_sum']}, "
                f"{arr_ide}, {arr_feat}, {arr_lang_feat}, {arr_lang_model}, {arr_model_feat})"
            )

    return [INSERT_SQL.format(catalog=catalog, table=TABLE, values=",\n".join(value_lines))]
