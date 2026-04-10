"""
generators/devex_meridian.py

Generates INSERT statements for Meridian DevEx tables:
  base_datasets.commits_rest_api   — commit activity
  base_datasets.pull_requests      — PR activity

Story arc (mirrors dora_meridian.py's inflection):
  Pre-Opsera (first half):
    Commits: 1-2 per user/weekday (manual data work, no CI discipline)
    Copilot:  lerp(0.05, 0.20, t_phase) — team just getting started
    PRs:      0-1/week total (barely any PRs; manual promotions via copy-paste)

  Post-Opsera (second half):
    Commits: 2-4 per user/weekday (disciplined engineering practice)
    Copilot:  lerp(0.20, 0.65, t_phase) — rapid adoption
    PRs:      lerp(1, 8, t_phase)/day across team (Opsera triggers PR-based deploys)

Deletion scoped by:
  commits_rest_api : org_name = 'demo-meridian'
  pull_requests    : merge_request_id LIKE 'meridian-seed-pr-%'
"""
import random
from datetime import date, datetime, timedelta

from .utils import date_range, lerp, _sql_val, split_across
from .dora_meridian import _build_months, _phase_t

TABLES = ("commits_rest_api", "pull_requests")

_COMMITS_SQL = """\
INSERT INTO {catalog}.base_datasets.commits_rest_api
  (commit_id, commit_date, commit_timestamp, org_name, project_name, project_url,
   cleansed_user_name, cleansed_commit_author, user_id, copilot_commit_flag,
   lines_added, lines_removed, before_sha, has_ticket_id, file_extension, web_url)
VALUES
{values};"""

_PR_SQL = """\
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
   pr_source, object_kind,
   latest_pr_review_state, first_pr_approved_timestamp)
VALUES
{values};"""

_REPO_NAME = "demo-meridian/data-platform"
_REPO_URL  = "https://github.com/demo-meridian/data-platform.git"
_PR_PREFIX = "meridian-seed-pr-"


def _fake_sha(seed: int) -> str:
    rng = random.Random(seed)
    return "".join(rng.choices("0123456789abcdef", k=40))


def _last_weekday_before(d: date) -> date:
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def _pr_commits_sql(rng, user, n_commits, pr_date, copilot_flag, first_sha, first_commit_day):
    structs = []
    for i in range(n_commits):
        if i == 0:
            sha = first_sha
            commit_day = first_commit_day
        else:
            commit_day = pr_date - timedelta(days=rng.randint(1, 2))
            sha = _fake_sha(hash((str(pr_date), user["id"], i, "meridian-pr")) % (2**31))

        commit_ts = f"{commit_day.isoformat()} {rng.randint(9, 17):02d}:{rng.randint(0, 59):02d}:00"
        lines_add = str(rng.randint(10, 150))
        lines_rem = str(rng.randint(0, 50))
        lines_mod = str(int(lines_add) + int(lines_rem))
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


def generate(catalog: str, entities: dict, story: dict) -> dict[str, list[str]]:
    """
    Returns a dict with keys 'commits' and 'prs', each a list of SQL strings.
    """
    org_name   = entities["orgs"][0]["name"]
    all_users  = entities["users"]
    start      = date.fromisoformat(story["start_date"])
    end        = date.fromisoformat(story["end_date"])

    months         = _build_months(start, end)
    total_months   = len(months)
    inflection_idx = total_months // 2

    # Build a per-day lookup: (date) -> (phase, t_within_phase)
    # so we can look up arc parameters without re-computing months each day
    _month_phase: dict[tuple[int, int], tuple[str, float]] = {}
    for idx, (yr, mo) in enumerate(months):
        phase   = "pre" if idx < inflection_idx else "post"
        t_phase = _phase_t(idx, inflection_idx, total_months)
        _month_phase[(yr, mo)] = (phase, t_phase)

    commit_lines = []
    pr_lines     = []
    pr_counter   = 1

    for d in date_range(story["start_date"], story["end_date"]):
        if d.weekday() >= 5:
            continue

        phase, t_phase = _month_phase.get((d.year, d.month), ("post", 1.0))

        # ── Commits ─────────────────────────────────────────────────────────────
        if phase == "pre":
            copilot_rate = lerp(0.05, 0.20, t_phase)
            commits_lo, commits_hi = 1, 2
        else:
            copilot_rate = lerp(0.20, 0.65, t_phase)
            commits_lo, commits_hi = 2, 4

        for user in all_users:
            day_rng = random.Random(hash((str(d), user["id"], "m-commits")) % (2**31))
            n_commits = day_rng.randint(commits_lo, commits_hi)

            for seq in range(n_commits):
                c_seed = hash((str(d), user["id"], seq, "meridian-commit")) % (2**31)
                c_rng  = random.Random(c_seed)
                commit_id  = _fake_sha(c_seed)
                before_sha = _fake_sha(c_seed + 1)
                copilot_flag = "Y" if c_rng.random() < copilot_rate else "N"

                t_global = max(0.0, min(1.0, (d - start).days / max((end - start).days, 1)))
                add_lo = round(lerp(10, 60, t_global))
                add_hi = round(lerp(60, 200, t_global))
                rem_hi = round(lerp(20, 70, t_global))
                lines_added   = c_rng.randint(add_lo, add_hi)
                lines_removed = c_rng.randint(0, rem_hi)
                commit_hour   = c_rng.randint(9, 17)
                commit_ts     = f"{d.isoformat()} {commit_hour:02d}:00:00"

                n_files   = c_rng.randint(1, 3)
                file_adds = split_across(lines_added, n_files)
                file_dels = split_across(lines_removed, n_files)
                file_structs = [
                    f"NAMED_STRUCT('file_extension', 'py', "
                    f"'lines', NAMED_STRUCT('additions', {file_adds[i]}, "
                    f"'changes', {file_adds[i] + file_dels[i]}, "
                    f"'deletions', {file_dels[i]}))"
                    for i in range(n_files)
                ]
                file_ext_sql = f"ARRAY({', '.join(file_structs)})"
                has_ticket   = 1 if c_rng.random() < 0.38 else 0
                web_url      = f"{_REPO_URL.replace('.git', '')}/commit/{commit_id}"

                commit_lines.append(
                    f"  ({_sql_val(commit_id)}, {_sql_val(d)}, "
                    f"TIMESTAMP '{commit_ts}', "
                    f"{_sql_val(org_name)}, {_sql_val(_REPO_NAME)}, {_sql_val(_REPO_URL)}, "
                    f"{_sql_val(user['login'])}, {_sql_val(user['login'])}, "
                    f"{user['id']}, {_sql_val(copilot_flag)}, "
                    f"{lines_added}, {lines_removed}, "
                    f"{_sql_val(before_sha)}, {has_ticket}, {file_ext_sql}, {_sql_val(web_url)})"
                )

        # ── Pull Requests ────────────────────────────────────────────────────────
        day_rng = random.Random(hash((str(d), "m-pr-count")) % (2**31))
        if phase == "pre":
            # ~0.2 PRs/day = ~1/week
            n_prs = day_rng.choices([0, 1], weights=[80, 20])[0]
            copilot_rate_pr = lerp(0.05, 0.20, t_phase)
            merge_days_range = (3, 10)
        else:
            # ramp from 1 → 8 PRs/day
            n_prs = max(0, round(lerp(1, 8, t_phase))) + day_rng.randint(-1, 1)
            n_prs = max(0, n_prs)
            copilot_rate_pr = lerp(0.20, 0.65, t_phase)
            merge_days_range = (1, 4)

        for seq in range(n_prs):
            rng = random.Random(hash((str(d), seq, "meridian-pr-row")) % (2**31))
            user = all_users[rng.randint(0, len(all_users) - 1)]

            pr_id  = f"{_PR_PREFIX}{pr_counter:06d}"
            pr_counter += 1
            pr_url = f"{_REPO_URL.replace('.git', '')}/pull/{pr_counter}"
            title  = f"feat: data-platform update ({pr_id})"

            copilot_flag = "Y" if rng.random() < copilot_rate_pr else "N"
            created_hour = rng.randint(9, 16)
            created_ts   = f"{d.isoformat()} {created_hour:02d}:00:00"

            days_to_merge = rng.randint(*merge_days_range)
            merge_date    = d + timedelta(days=days_to_merge)

            if merge_date <= end and rng.random() < 0.80:
                pr_state     = "closed"
                merge_status = True
                merged_ts    = f"{merge_date.isoformat()} {rng.randint(10, 18):02d}:00:00"
                closed_ts    = merged_ts
            else:
                pr_state     = "open"
                merge_status = False
                merged_ts    = None
                closed_ts    = None

            first_commit_day  = _last_weekday_before(d)
            first_commit_seed = hash((str(first_commit_day), user["id"], 0, "meridian-commit")) % (2**31)
            first_commit_sha  = _fake_sha(first_commit_seed)

            n_pr_commits = rng.randint(2, 5)
            commits_sql  = _pr_commits_sql(
                rng, user, n_pr_commits, d, copilot_flag,
                first_sha=first_commit_sha, first_commit_day=first_commit_day,
            )

            pickup_hours = rng.randint(2, 8) if copilot_flag == "Y" else rng.randint(4, 24)
            created_dt   = datetime.strptime(created_ts, "%Y-%m-%d %H:%M:%S")
            review_dt    = created_dt + timedelta(hours=pickup_hours)
            if pr_state == "closed" and merged_ts:
                merged_dt = datetime.strptime(merged_ts, "%Y-%m-%d %H:%M:%S")
                if review_dt >= merged_dt:
                    review_dt = merged_dt - timedelta(hours=1)
            review_sql = f"TIMESTAMP '{review_dt.strftime('%Y-%m-%d %H:%M:%S')}'"

            lines_add = str(rng.randint(50, 500))
            lines_rem = str(rng.randint(10, 150))

            merged_ts_sql = f"TIMESTAMP '{merged_ts}'" if merged_ts else "NULL"
            merged_d_sql  = f"DATE '{merge_date.isoformat()}'" if merged_ts else "NULL"
            closed_ts_sql = f"TIMESTAMP '{closed_ts}'" if closed_ts else "NULL"
            closed_d_sql  = f"DATE '{merge_date.isoformat()}'" if closed_ts else "NULL"

            if pr_state == "closed" and merge_status:
                review_state_sql = "'APPROVED'"
                approved_ts_sql  = merged_ts_sql
            else:
                review_state_sql = "'OPEN'"
                approved_ts_sql  = "NULL"

            pr_lines.append(
                f"  ({_sql_val(org_name)}, {_sql_val(_REPO_NAME)}, {_sql_val(_REPO_URL)}, "
                f"{_sql_val(user['login'])}, {_sql_val(user['login'])}, "
                f"{user['id']}, "
                f"{_sql_val(pr_id)}, {_sql_val(pr_url)}, {_sql_val(title)}, "
                f"{_sql_val(pr_state)}, {str(merge_status).upper()}, "
                f"TIMESTAMP '{created_ts}', DATE '{d.isoformat()}', "
                f"{merged_ts_sql}, {merged_d_sql}, "
                f"{closed_ts_sql}, {closed_d_sql}, "
                f"{_sql_val(first_commit_sha)}, "
                f"{review_sql}, "
                f"{commits_sql}, "
                f"'feature/{user['login']}-patch', 'main', "
                f"{_sql_val(lines_add)}, {_sql_val(lines_rem)}, "
                f"'github', 'pull_request', "
                f"{review_state_sql}, {approved_ts_sql})"
            )

    # Build SQL statements, chunked to avoid oversized INSERTs
    chunk_size = 500

    commit_stmts = []
    for i in range(0, len(commit_lines), chunk_size):
        commit_stmts.append(
            _COMMITS_SQL.format(catalog=catalog, values=",\n".join(commit_lines[i:i + chunk_size]))
        )

    pr_stmts = []
    for i in range(0, len(pr_lines), chunk_size):
        pr_stmts.append(
            _PR_SQL.format(catalog=catalog, values=",\n".join(pr_lines[i:i + chunk_size]))
        )

    return {"commits": commit_stmts, "prs": pr_stmts}
