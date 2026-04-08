"""
Generator for base_datasets.pull_requests.
Creates both Copilot and Non-Copilot PRs for demo-acme-direct.

Each PR embeds a pr_commits array with copilot_commit_flag set per commit.
The query (pr_activity.sql) classifies PRs as:
  - Copilot      : at least one commit with copilot_commit_flag = 'Y'
  - Non-Copilot  : zero copilot commits
  - Mixed        : both copilot and non-copilot commits

Story arc:
  - Non-Copilot PRs: stable baseline of 3-4/day throughout
  - Copilot PRs: ramps from ~1/day to ~5/day matching commit adoption curve
  - Copilot PRs merge faster (1-3 days) vs Non-Copilot (3-8 days)

Deletion is scoped via merge_request_id LIKE 'demo-seed-pr-%'
so existing rows from other orgs are never touched.
"""
import random
from datetime import date, datetime, timedelta
from .utils import date_range, expand_users, active_user_count, lerp, _sql_val

TABLE = "pull_requests"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.pull_requests
  (org_name, project_name, project_url, pr_user_name, merge_request_user,
   pr_user_id,
   merge_request_id, merge_request_url, merge_request_title,
   pr_state, merge_status,
   pr_created_datetime, pr_created_date,
   pr_merged_datetime, pr_merged_date,
   pr_closed_datetime, pr_closed_date,
   first_commit_id,
   first_pr_review_submitted_datetime,
   pr_commits,
   source_branch, target_branch,
   lines_added, lines_removed,
   pr_source, object_kind)
VALUES
{values};"""

_REPOS = [
    ("demo-acme-direct/backend",     "https://github.com/demo-acme-direct/backend"),
    ("demo-acme-direct/frontend",    "https://github.com/demo-acme-direct/frontend"),
    ("demo-acme-direct/api-gateway", "https://github.com/demo-acme-direct/api-gateway"),
]

# Copilot adoption: fraction of PRs that are Copilot, grows 20% → 65%
_COPILOT_RATE = (0.20, 0.65)

# Days to merge: Copilot PRs are faster
_MERGE_DAYS_COPILOT     = (1, 3)
_MERGE_DAYS_NON_COPILOT = (3, 8)


def _fake_sha(seed: int) -> str:
    rng = random.Random(seed)
    return "".join(rng.choices("0123456789abcdef", k=40))


def _last_weekday_before(d: date) -> date:
    """Return the most recent weekday strictly before d."""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def _pr_commits_sql(rng: random.Random, user: dict, n_commits: int,
                    pr_date: date, copilot_flag: str,
                    first_sha: str, first_commit_day: date) -> str:
    """Build an ARRAY of pr_commit NAMED_STRUCTs.

    first_sha / first_commit_day are seeded to match commits_rest_api so
    the commit-to-PR join (pr.first_commit_id = c.commit_id) succeeds.
    All commits are timestamped before pr_date so coding_time > 0.
    """
    structs = []
    for i in range(n_commits):
        if i == 0:
            sha = first_sha
            commit_day = first_commit_day
        else:
            # Additional commits: 1-2 days before PR creation
            commit_day = pr_date - timedelta(days=rng.randint(1, 2))
            sha = _fake_sha(hash((str(pr_date), user["id"], i, "pr")) % (2**31))

        commit_ts  = f"{commit_day.isoformat()} {rng.randint(9, 17):02d}:{rng.randint(0,59):02d}:00"
        lines_add  = str(rng.randint(10, 150))
        lines_rem  = str(rng.randint(0,  50))
        lines_mod  = str(int(lines_add) + int(lines_rem))
        structs.append(
            f"NAMED_STRUCT("
            f"'sha', {_sql_val(sha)}, "
            f"'commit_timestamp', {_sql_val(commit_ts)}, "
            f"'commit_author', {_sql_val(user['login'])}, "
            f"'author_id', {_sql_val(str(user['id']))}, "
            f"'author_login', {_sql_val(user['login'])}, "
            f"'lines_added', {_sql_val(lines_add)}, "
            f"'lines_removed', {_sql_val(lines_rem)}, "
            f"'lines_modified', {_sql_val(lines_mod)}, "
            f"'cleansed_commit_author_name', {_sql_val(user['login'])}, "
            f"'cleansed_author_login', {_sql_val(user['login'])}, "
            f"'copilot_commit_flag', {_sql_val(copilot_flag)})"
        )
    return f"ARRAY({', '.join(structs)})"


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]
    all_users = expand_users(entities, story)
    start     = date.fromisoformat(story["start_date"])
    end       = date.fromisoformat(story["end_date"])
    total_days = max((end - start).days, 1)

    pr_counter = 1
    value_lines = []

    for d in date_range(story["start_date"], story["end_date"]):
        if d.weekday() >= 5:
            continue

        active_n = active_user_count(d, story, len(all_users))
        if active_n == 0:
            continue

        t = max(0.0, min(1.0, (d - start).days / total_days))
        copilot_rate = lerp(_COPILOT_RATE[0], _COPILOT_RATE[1], t)

        # ~3-4 Non-Copilot PRs/day (stable) + growing Copilot PRs
        day_rng = random.Random(hash((str(d), "pr_count")) % (2**31))
        n_non_copilot = day_rng.randint(2, 4)
        n_copilot     = round(lerp(0.5, 4.0, t)) + day_rng.randint(0, 1)

        for pr_type, n_prs in [("N", n_non_copilot), ("Y", n_copilot)]:
            for seq in range(n_prs):
                rng = random.Random(
                    hash((str(d), pr_type, seq, "pr_row")) % (2**31)
                )
                user = all_users[rng.randint(0, min(active_n, len(all_users)) - 1)]
                repo_name, repo_url = rng.choice(_REPOS)

                pr_id  = f"demo-seed-pr-{pr_counter:06d}"
                pr_counter += 1
                pr_url = f"{repo_url}/pull/{pr_counter}"
                title  = f"feat: update {repo_name.split('/')[1]} ({pr_id})"

                created_hour = rng.randint(9, 16)
                created_ts   = f"{d.isoformat()} {created_hour:02d}:00:00"

                # Determine merge outcome
                merge_window = _MERGE_DAYS_COPILOT if pr_type == "Y" else _MERGE_DAYS_NON_COPILOT
                days_to_merge = rng.randint(*merge_window)
                merge_date    = d + timedelta(days=days_to_merge)

                # PRs that would merge after end_date stay open
                if merge_date <= end and rng.random() < 0.80:
                    pr_state   = "closed"
                    merge_status = True
                    merged_ts  = f"{merge_date.isoformat()} {rng.randint(10, 18):02d}:00:00"
                    closed_ts  = merged_ts
                else:
                    pr_state   = "open"
                    merge_status = False
                    merged_ts  = None
                    closed_ts  = None

                # first_commit matches commits_rest_api: same seed formula as commits.py
                first_commit_day = _last_weekday_before(d)
                first_commit_seed = hash((str(first_commit_day), user["id"], 0)) % (2**31)
                first_commit_sha = _fake_sha(first_commit_seed)

                n_commits   = rng.randint(2, 6)
                commits_sql = _pr_commits_sql(
                    rng, user, n_commits, d, pr_type,
                    first_sha=first_commit_sha, first_commit_day=first_commit_day,
                )

                # first_pr_review_submitted_datetime — pickup time after PR creation
                pickup_hours = rng.randint(2, 8) if pr_type == "Y" else rng.randint(4, 24)
                created_dt = datetime.strptime(created_ts, "%Y-%m-%d %H:%M:%S")
                review_submitted_dt = created_dt + timedelta(hours=pickup_hours)
                if pr_state == "closed" and merged_ts:
                    merged_dt = datetime.strptime(merged_ts, "%Y-%m-%d %H:%M:%S")
                    if review_submitted_dt >= merged_dt:
                        review_submitted_dt = merged_dt - timedelta(hours=1)
                review_submitted_sql = f"TIMESTAMP '{review_submitted_dt.strftime('%Y-%m-%d %H:%M:%S')}'"

                lines_add = str(rng.randint(50, 500))
                lines_rem = str(rng.randint(10, 150))

                merged_ts_sql  = f"TIMESTAMP '{merged_ts}'" if merged_ts  else "NULL"
                merged_d_sql   = f"DATE '{merge_date.isoformat()}'" if merged_ts else "NULL"
                closed_ts_sql  = f"TIMESTAMP '{closed_ts}'" if closed_ts  else "NULL"
                closed_d_sql   = f"DATE '{merge_date.isoformat()}'" if closed_ts else "NULL"

                value_lines.append(
                    f"  ({_sql_val(org_name)}, {_sql_val(repo_name)}, {_sql_val(repo_url)}, "
                    f"{_sql_val(user['login'])}, {_sql_val(user['login'])}, "
                    f"{user['id']}, "
                    f"{_sql_val(pr_id)}, {_sql_val(pr_url)}, {_sql_val(title)}, "
                    f"{_sql_val(pr_state)}, {str(merge_status).upper()}, "
                    f"TIMESTAMP '{created_ts}', DATE '{d.isoformat()}', "
                    f"{merged_ts_sql}, {merged_d_sql}, "
                    f"{closed_ts_sql}, {closed_d_sql}, "
                    f"{_sql_val(first_commit_sha)}, "
                    f"{review_submitted_sql}, "
                    f"{commits_sql}, "
                    f"'feature/{user['login']}-patch', 'main', "
                    f"{_sql_val(lines_add)}, {_sql_val(lines_rem)}, "
                    f"'github', 'pull_request')"
                )

    # Batch into chunks to avoid single enormous SQL statements
    chunk_size = 500
    statements = []
    for i in range(0, len(value_lines), chunk_size):
        chunk = value_lines[i:i + chunk_size]
        statements.append(
            INSERT_SQL.format(catalog=catalog, values=",\n".join(chunk))
        )
    return statements
