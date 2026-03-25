"""
Generator for enterprise_level_copilot_metrics.
One row per (usage_date) — rolls up all users for the enterprise.
Must be generated AFTER user_level so totals are consistent.
"""
from datetime import date, datetime, timezone
from .utils import (
    date_range, _sql_val, sql_array,
    totals_by_feature_entry,
    totals_by_ide_entry_enterprise,
    totals_by_language_feature_entry,
    totals_by_language_model_entry,
    totals_by_model_feature_entry,
)


TABLE = "enterprise_level_copilot_metrics"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.{table}
  (enterprise_id, enterprise, created_at, usage_date,
   code_acceptance_activity_count, code_generation_activity_count,
   daily_active_users,
   loc_added_sum, loc_deleted_sum, loc_suggested_to_add_sum, loc_suggested_to_delete_sum,
   monthly_active_agent_users, monthly_active_chat_users, monthly_active_users,
   totals_by_feature, totals_by_ide, totals_by_language_feature,
   totals_by_language_model, totals_by_model_feature,
   user_initiated_interaction_count, weekly_active_users)
VALUES
{values};"""


def generate(catalog: str, entities: dict, story: dict, user_rows: list[dict]) -> list[str]:
    """
    user_rows: list of dicts produced by user_level generator, one per (date, user).
    We aggregate them here so enterprise totals are always consistent with user totals.
    """
    ent = entities["enterprise"]
    active_features = story["active_features"]
    languages = entities["languages"]
    models = entities["models"]
    users = entities["users"]
    user_ide_map = story["user_ide_map"]

    # Group user rows by date
    by_date: dict[str, list[dict]] = {}
    for row in user_rows:
        by_date.setdefault(row["usage_date"], []).append(row)

    created_at = f"TIMESTAMP '{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000+00:00')}'"

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        d_str = str(d)
        day_rows = by_date.get(d_str, [])

        # Aggregate scalar totals across all users for this day
        total_code_gen = sum(r["code_generation_activity_count"] for r in day_rows)
        total_code_acc = sum(r["code_acceptance_activity_count"] for r in day_rows)
        total_loc_add = sum(r["loc_added_sum"] for r in day_rows)
        total_loc_del = sum(r["loc_deleted_sum"] for r in day_rows)
        total_loc_sugg_add = sum(r["loc_suggested_to_add_sum"] for r in day_rows)
        total_loc_sugg_del = sum(r["loc_suggested_to_delete_sum"] for r in day_rows)
        total_interactions = sum(r["user_initiated_interaction_count"] for r in day_rows)

        n_users = len(users)
        daily_active = n_users
        weekly_active = n_users
        monthly_active = n_users
        monthly_chat = sum(1 for r in day_rows if r.get("used_chat"))
        monthly_agent = 0  # Phase 1: no agent usage

        # Build nested arrays — one entry per IDE used across users
        ides_used = list({user_ide_map.get(u["login"], "vscode") for u in users})
        ide_entries = []
        for ide in ides_used:
            ide_users = [r for r in day_rows if r.get("ide") == ide]
            ide_gen = sum(r["code_generation_activity_count"] for r in ide_users) or total_code_gen // len(ides_used)
            ide_acc = sum(r["code_acceptance_activity_count"] for r in ide_users) or total_code_acc // len(ides_used)
            ide_interactions = sum(r["user_initiated_interaction_count"] for r in ide_users) or total_interactions // len(ides_used)
            ide_entries.append(totals_by_ide_entry_enterprise(
                ide=ide,
                code_gen=ide_gen,
                code_accept=ide_acc,
                interaction_count=ide_interactions,
                accepted_loc=ide_acc,
                generated_loc=ide_gen,
            ))

        # One entry per active feature
        n_feat = len(active_features)
        feature_entries = [
            totals_by_feature_entry(
                feature=f,
                code_gen=total_code_gen // n_feat,
                code_accept=total_code_acc // n_feat,
                interaction_count=total_interactions // n_feat,
                accepted_loc=total_loc_add // n_feat,
                generated_loc=total_loc_sugg_add // n_feat,
            )
            for f in active_features
        ]

        # One entry per language+feature combo
        n_lang = len(languages)
        lang_feature_entries = [
            totals_by_language_feature_entry(
                language=languages[li % n_lang],
                feature=f,
                code_gen=total_code_gen // (n_feat * n_lang),
                code_accept=total_code_acc // (n_feat * n_lang),
                accepted_loc=total_loc_add // (n_feat * n_lang),
                generated_loc=total_loc_sugg_add // (n_feat * n_lang),
            )
            for li, f in enumerate(active_features)
        ]

        # One entry per language+model combo
        n_model = len(models)
        lang_model_entries = [
            totals_by_language_model_entry(
                language=languages[mi % n_lang],
                model=models[mi % n_model],
                code_gen=total_code_gen // n_model,
                code_accept=total_code_acc // n_model,
                accepted_loc=total_loc_add // n_model,
                generated_loc=total_loc_sugg_add // n_model,
            )
            for mi in range(n_model)
        ]

        # One entry per model+feature combo
        model_feature_entries = [
            totals_by_model_feature_entry(
                model=models[fi % n_model],
                feature=f,
                code_gen=total_code_gen // n_feat,
                code_accept=total_code_acc // n_feat,
                interaction_count=total_interactions // n_feat,
                accepted_loc=total_loc_add // n_feat,
                generated_loc=total_loc_sugg_add // n_feat,
            )
            for fi, f in enumerate(active_features)
        ]

        value_lines.append(
            f"  ({ent['id']}, {_sql_val(ent['name'])}, {created_at}, {_sql_val(d)}, "
            f"{total_code_acc}, {total_code_gen}, "
            f"{daily_active}, "
            f"{total_loc_add}, {total_loc_del}, {total_loc_sugg_add}, {total_loc_sugg_del}, "
            f"{monthly_agent}, {monthly_chat}, {monthly_active}, "
            f"{sql_array(feature_entries)}, {sql_array(ide_entries)}, {sql_array(lang_feature_entries)}, "
            f"{sql_array(lang_model_entries)}, {sql_array(model_feature_entries)}, "
            f"{total_interactions}, {weekly_active})"
        )

    return [INSERT_SQL.format(catalog=catalog, table=TABLE, values=",\n".join(value_lines))]
