"""
Generator for trf_github_copilot_direct_data.
One row per (date, user_login, application) where the user engaged with that feature.
Drives: Feature Active Users, Feature Diversity Score, Sustained Engagement Rate.
"""
import random
from datetime import date
from .utils import date_range, expand_users, active_user_count, lerp, _sql_val, incident_multiplier


TABLE = "trf_github_copilot_direct_data"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.{table}
  (date, user_login, team_name, application, org_name, enterprise_id)
VALUES
{values};"""

# Fraction of active users that engage with each feature on a given day.
# Each value is (start_rate, end_rate) — linearly interpolated over the story range.
_FEATURE_ADOPTION = {
    "code_completion":      (1.00, 1.00),  # all active users, every day
    "chat_panel_ask_mode":  (0.40, 0.80),
    "chat_panel_edit_mode": (0.20, 0.60),
    "agent_edit":           (0.05, 0.40),  # starts low as agent adoption grows
}


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    ent = entities["enterprise"]
    org_name = entities["orgs"][0]["name"]
    all_users = expand_users(entities, story)
    active_features = story["active_features"]
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])
    total_days = max((end - start).days, 1)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        n = active_user_count(d, story, len(all_users))
        if n == 0:
            continue
        inc_mult = incident_multiplier(d)
        if inc_mult != 1.0:
            n = max(0, min(len(all_users), round(n * inc_mult)))
        if n == 0:
            continue
        t = max(0.0, min(1.0, (d - start).days / total_days))
        for user in all_users[:n]:
            for feature in active_features:
                rate_start, rate_end = _FEATURE_ADOPTION.get(feature, (0.5, 0.5))
                adoption_rate = lerp(rate_start, rate_end, t)
                seed = hash((str(d), user["id"], feature)) % (2 ** 31)
                if random.Random(seed).random() > adoption_rate:
                    continue
                value_lines.append(
                    f"  ({_sql_val(d)}, {_sql_val(user['login'])}, "
                    f"{_sql_val(user['team'])}, {_sql_val(feature)}, "
                    f"{_sql_val(org_name)}, {ent['id']})"
                )

    return [INSERT_SQL.format(catalog=catalog, table=TABLE, values=",\n".join(value_lines))]
