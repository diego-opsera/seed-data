"""
Generator for commits_rest_api.
One row per commit. Active users make 2–4 commits per business day.
copilot_commit_flag ramps from ~20% → ~65% over the year, showing growing adoption.
before_sha is a single parent SHA so queries filtering merge commits (size = 1) include all rows.
Drives: Commit Trend, Commit Overview.
"""
import random
from datetime import date
from .utils import date_range, expand_users, active_user_count, lerp, _sql_val, split_across


TABLE = "commits_rest_api"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.{table}
  (commit_id, commit_date, commit_timestamp, org_name, project_name, project_url,
   cleansed_user_name, cleansed_commit_author, user_id, copilot_commit_flag,
   lines_added, lines_removed, before_sha, has_ticket_id, file_extension, web_url)
VALUES
{values};"""

# Maps user language to primary file extension (must match master_data.file_extensions)
_LANG_EXT = {
    "typescript": "ts",
    "python":     "py",
    "go":         "go",
    "csharp":     "cs",
    "elixir":     "ex",
}

# Projects mapped to user teams
_TEAM_PROJECT = {
    "demo-frontend": ("demo-acme-direct/frontend",    "https://github.com/demo-acme-direct/frontend.git"),
    "demo-backend":  ("demo-acme-direct/backend",     "https://github.com/demo-acme-direct/backend.git"),
}
_DEFAULT_PROJECT = ("demo-acme-direct/api-gateway", "https://github.com/demo-acme-direct/api-gateway.git")

# Copilot attribution grows from 20% → 65% over the year
_COPILOT_FLAG_RATE = (0.20, 0.65)


def _fake_sha(seed: int) -> str:
    """Deterministic 40-char hex string that looks like a git SHA."""
    rng = random.Random(seed)
    return "".join(rng.choices("0123456789abcdef", k=40))


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    ent = entities["enterprise"]
    org_name = entities["orgs"][0]["name"]
    all_users = expand_users(entities, story)
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])
    total_days = max((end - start).days, 1)

    value_lines = []
    for d in date_range(story["start_date"], story["end_date"]):
        if d.weekday() >= 5:  # commits happen on weekdays; weekend spikes handled by active_user_count
            continue
        active_n = active_user_count(d, story, len(all_users))
        if active_n == 0:
            continue
        t = max(0.0, min(1.0, (d - start).days / total_days))
        copilot_rate = lerp(_COPILOT_FLAG_RATE[0], _COPILOT_FLAG_RATE[1], t)

        for user in all_users[:active_n]:
            project_name, project_url = _TEAM_PROJECT.get(user["team"], _DEFAULT_PROJECT)
            day_rng = random.Random(hash((str(d), user["id"], "commits")) % (2 ** 31))
            n_commits = day_rng.randint(2, 4)

            for seq in range(n_commits):
                c_seed = hash((str(d), user["id"], seq)) % (2 ** 31)
                c_rng  = random.Random(c_seed)
                commit_id  = _fake_sha(c_seed)
                before_sha = _fake_sha(c_seed + 1)  # one parent = non-merge commit
                copilot_flag = "Y" if c_rng.random() < copilot_rate else "N"
                # Scale line counts with story trend so total commit lines stay ~2x
                # copilot accepted lines throughout the year (accepted lines grow 4x as
                # loc_suggested_to_add ramps from 75→300).
                add_lo = round(lerp(10,  60, t))
                add_hi = round(lerp(60, 200, t))
                rem_hi = round(lerp(20,  70, t))
                lines_added   = c_rng.randint(add_lo, add_hi)
                lines_removed = c_rng.randint(0, rem_hi)
                commit_hour   = c_rng.randint(9, 17)
                commit_ts     = f"{d.isoformat()} {commit_hour:02d}:00:00"

                ext = _LANG_EXT.get(user.get("language", "python"), "py")
                n_files = c_rng.randint(1, 3)
                file_adds = split_across(lines_added, n_files)
                file_dels = split_across(lines_removed, n_files)
                file_structs = [
                    f"NAMED_STRUCT('file_extension', '{ext}', "
                    f"'lines', NAMED_STRUCT('additions', {file_adds[i]}, "
                    f"'changes', {file_adds[i] + file_dels[i]}, "
                    f"'deletions', {file_dels[i]}))"
                    for i in range(n_files)
                ]
                file_ext_sql = f"ARRAY({', '.join(file_structs)})"

                has_ticket = 1 if c_rng.random() < 0.38 else 0
                # web_url needed for COUNT(DISTINCT commit_id, web_url) in commit_statistics SQL
                repo_base = project_url.replace(".git", "")
                web_url   = f"{repo_base}/commit/{commit_id}"

                value_lines.append(
                    f"  ({_sql_val(commit_id)}, {_sql_val(d)}, "
                    f"TIMESTAMP '{commit_ts}', "
                    f"{_sql_val(org_name)}, {_sql_val(project_name)}, {_sql_val(project_url)}, "
                    f"{_sql_val(user['login'])}, {_sql_val(user['login'])}, "
                    f"{user['id']}, {_sql_val(copilot_flag)}, "
                    f"{lines_added}, {lines_removed}, "
                    f"{_sql_val(before_sha)}, {has_ticket}, {file_ext_sql}, {_sql_val(web_url)})"
                )

    return [INSERT_SQL.format(catalog=catalog, table=TABLE, values=",\n".join(value_lines))]
