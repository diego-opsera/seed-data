"""
Generator for consumption_layer.commits_prs.
One row per commit (with optional PR linkage + AI-tool tagging) — drives:
  - Commit Integration Rate (ai_commits / total_commits per tool)
  - Tool Comparison Matrix Scoreboard:
      * velocity   = first-commit → PR-creation time per ai_tool
      * quality    = bug-ticketed commits per ai_tool (joins v_itsm_issues_hist)
      * security   = via pipeline_activities → asp_sonar_* (we don't link these,
                     so security column will be 0 for now — see note below)

ai_tool ARRAY tagging:
  Each commit gets the union of tools its committer is licensed for, with a
  per-commit probability of "leaving a fingerprint" derived from the tool's
  active_share. A power user on all 3 tools may end up with ai_tool=['github
  copilot','cursor','claude code'] on the same commit; a copilot-only dev
  gets ai_tool=['github copilot'] or empty (~30% of the time).

commit_tickets ARRAY:
  ~25% of commits reference 1 ITSM ticket. Issue keys are deterministic
  (ACME-N) from itsm_issues.py — itsm generates ~9-13 issues per ~3-week
  sprint, so ACME-1..ACME-250 is a safe range for the year window.

Security note:
  Tying commits → pipeline_activities → sonar_measures requires matching
  merge_commit_sha to pipeline_commit_sha. The existing dora/dora_charts
  generators don't expose those SHAs, so the scoreboard's "security" column
  will read 0. Adding that linkage is out of scope for v1.

Deletion scoped to org_name = 'demo-acme-direct'.
"""
import hashlib
import random
from datetime import date, timedelta
from .utils import (
    date_range, expand_users, active_user_count, _sql_val,
    tool_is_live, assign_users_to_tool, incident_multiplier, day_scale,
)

TABLE  = "commits_prs"
SCHEMA = "consumption_layer"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.commits_prs
  (commit_id, commit_timestamp, commit_date, github_commit_email, org_name,
   pr_id, pr_created_datetime, pr_closed_datetime, merge_commit_sha,
   commit_tickets, ai_tool)
VALUES
{values};"""


def _sha(seed_str: str) -> str:
    """Deterministic 40-char hex SHA from a seed string (git-shape)."""
    return hashlib.sha1(seed_str.encode()).hexdigest()


def _array_literal(items: list[str]) -> str:
    if not items:
        return "CAST(ARRAY() AS ARRAY<STRING>)"
    return "ARRAY(" + ", ".join(_sql_val(i) for i in items) + ")"


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]
    tools     = entities.get("ai_tools", [])
    if not tools:
        return []
    all_users = expand_users(entities, story)

    # Tool rosters (same seed as ai_usage_user_level so memberships agree)
    tool_rosters = {t["name"]: set(u["id"] for u in assign_users_to_tool(all_users, t, seed=42))
                    for t in tools}

    # Per-commit tagging probability per tool — higher for tools the user
    # actively prefers. Use active_share as a rough proxy (Copilot 0.75,
    # Cursor 0.80, Claude Code 0.85 → commits get tagged ~that often when
    # the committer is on the tool).
    tag_prob = {t["name"]: float(t.get("active_share", 0.7)) for t in tools}

    # Pre-built pool of plausible ITSM bug issue_keys to use in commit_tickets.
    # itsm_issues.py generates ~1 epic + 8-12 issues per ~3-week sprint with
    # the story spanning 365 days → ~17 sprints → ~150-220 issues. We point
    # commit_tickets into the first 200 keys to stay safely inside the range.
    bug_ticket_keys = [f"ACME-{n}" for n in range(2, 200)]

    rng = random.Random(42)
    value_lines = []
    pr_counter = 1000

    # PR creation delay (in hours) after a commit, scaled inversely by the
    # tool's productivity_mult so higher-productivity tools yield faster
    # time-to-PR in the scoreboard velocity calc.
    def _pr_delay_hours(tags: list[str]) -> float:
        if not tags:
            return rng.uniform(4, 8)
        best_mult = max(
            (float(t["productivity_mult"]) for t in tools if t["name"] in tags),
            default=1.0,
        )
        base = rng.uniform(3, 7)
        return max(0.5, base / best_mult)

    for d in date_range(story["start_date"], story["end_date"]):
        baseline = active_user_count(d, story, len(all_users))
        if baseline == 0:
            continue
        inc_mult = incident_multiplier(d)
        scale = day_scale(d, story)
        if scale == 0:
            continue

        # ~12 commits per active-user-day on average, scaled by day_scale + incident
        n_commits = max(1, round(baseline * 0.6 * scale * inc_mult))

        for c_idx in range(n_commits):
            user = rng.choice(all_users[:baseline] or all_users)
            commit_seed = f"{d.isoformat()}-{c_idx}-{user['id']}"
            commit_id = _sha("c:" + commit_seed)
            hour = rng.randint(8, 18)
            minute = rng.randint(0, 59)
            commit_ts = f"{d.isoformat()} {hour:02d}:{minute:02d}:00"
            email = f"{user['login']}@demo-acme-direct.com"

            # Tag with tools the committer is on, weighted by tag_prob
            ai_tags = []
            for tool in tools:
                if user["id"] not in tool_rosters[tool["name"]]:
                    continue
                if not tool_is_live(tool, d, story):
                    continue
                if rng.random() < tag_prob[tool["name"]]:
                    ai_tags.append(tool["name"])

            # commit_tickets: ~25% chance of one bug-ticket reference
            tickets = []
            if rng.random() < 0.25:
                tickets.append(rng.choice(bug_ticket_keys))

            # PR linkage: ~35% of commits initiate or join a PR.
            # Half of new PRs get 1 commit (single-commit PRs), the other half
            # get 2-4 commits across this+next day.
            pr_id_val = None
            pr_created_dt = None
            pr_closed_dt = None
            merge_sha = None

            if rng.random() < 0.35:
                pr_counter += 1
                pr_id_val = f"PR-{pr_counter}"
                # PR created `_pr_delay_hours` after the first commit. We model
                # each commit as its own PR; the scoreboard's MIN(commit_ts)
                # and MIN(pr_created_dt) reduce to this commit's values, so
                # time_to_pr = delay_hours * 3600.
                delay_h = _pr_delay_hours(ai_tags)
                delay_min = int(delay_h * 60)
                created_total_min = hour * 60 + minute + delay_min
                created_d = d + timedelta(days=created_total_min // (24 * 60))
                created_min_of_day = created_total_min % (24 * 60)
                ch, cm = divmod(created_min_of_day, 60)
                pr_created_dt = f"{created_d.isoformat()} {ch:02d}:{cm:02d}:00"
                close_d = created_d + timedelta(days=rng.randint(1, 3))
                pr_closed_dt = f"{close_d.isoformat()} {rng.randint(10, 17):02d}:30:00"
                merge_sha = _sha("m:" + commit_seed)

            pr_created_sql = f"TIMESTAMP '{pr_created_dt}'" if pr_created_dt else "NULL"
            pr_closed_sql  = f"TIMESTAMP '{pr_closed_dt}'"  if pr_closed_dt  else "NULL"
            value_lines.append(
                f"  ({_sql_val(commit_id)}, TIMESTAMP '{commit_ts}', "
                f"DATE '{d.isoformat()}', {_sql_val(email)}, "
                f"{_sql_val(org_name)}, "
                f"{_sql_val(pr_id_val)}, "
                f"{pr_created_sql}, {pr_closed_sql}, "
                f"{_sql_val(merge_sha)}, "
                f"{_array_literal(tickets)}, "
                f"{_array_literal(ai_tags)})"
            )

    if not value_lines:
        return []
    # Chunk to keep each INSERT under the Spark SQL statement-size budget.
    batch_size = 500
    statements = []
    for i in range(0, len(value_lines), batch_size):
        batch = value_lines[i:i + batch_size]
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(batch)))
    return statements
