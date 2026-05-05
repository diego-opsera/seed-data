"""
generators/value_stream.py

Generates INSERT statements for `playground_prod.user_working.offerings_jira_pipeline_details`,
the denormalized fact table that powers the Issue Stream / Flow View feature in
vnxt-insights-api (src/queries/value-stream/*.sql).

Each Jira ticket produces multiple sparse rows. Each row carries the full ticket +
hierarchy context, but populates exactly one of {commit, PR, pipeline-step}:

  - 2-4 commit rows     (commit_* populated, pr_* and pipeline_* NULL)
  - 1-2 PR rows         (pr_* populated, commit_* and pipeline_* NULL)
  - 2 pipeline runs × 6 stage steps = 12 pipeline rows

Stage step (step_type, step_name) tuples cover all 6 LIKE filters in
issue-stream-list.sql so the badges render: build/security/quality/qa/deploy/production.

Configurable per org via OrgConfig — Acme has a steady ramp; Meridian has the same
manual→Opsera inflection arc as the DORA charts.
"""

import hashlib
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from .utils import lerp


TABLE = "user_working.offerings_jira_pipeline_details"


# ── Org configuration ─────────────────────────────────────────────────────────

@dataclass
class OrgConfig:
    sbg: str
    gbe: str
    offering: str
    org_name: str
    project_name: str
    jira_project: str
    project_url: str
    sprint_prefix: str
    sprint_count: int
    sprint_length_days: int
    ticket_id_start: int
    ticket_count: int
    inflection: bool
    pre_count: int
    post_count: int
    copilot_pct_start: float
    copilot_pct_end: float
    pipeline_failure_pct_start: float
    pipeline_failure_pct_end: float


# Acme: steady ramp, 35 tickets, 12 monthly sprints
ACME = OrgConfig(
    sbg="Acme Corp",
    gbe="Engineering",
    offering="demo-acme-corp",
    org_name="demo-acme-direct",
    project_name="demo-acme-direct/backend",
    jira_project="ACME",
    project_url="https://github.com/demo-acme-direct/backend",
    sprint_prefix="ACME Sprint",
    sprint_count=12,
    sprint_length_days=30,
    ticket_id_start=1001,
    ticket_count=35,
    inflection=False,
    pre_count=0,
    post_count=0,
    copilot_pct_start=0.10,
    copilot_pct_end=0.60,
    pipeline_failure_pct_start=0.05,
    pipeline_failure_pct_end=0.05,
)


# Meridian: inflection at t=0.5 — pre-Opsera (slow, high-fail) → post-Opsera (fast, low-fail)
MERIDIAN = OrgConfig(
    sbg="Meridian Analytics",
    gbe="Data",
    offering="demo-meridian",
    org_name="demo-meridian",
    project_name="demo-meridian/data-platform",
    jira_project="MDP",
    project_url="https://github.com/demo-meridian/data-platform",
    sprint_prefix="MDP Sprint",
    sprint_count=26,
    sprint_length_days=14,
    ticket_id_start=2001,
    ticket_count=40,
    inflection=True,
    pre_count=10,
    post_count=30,
    copilot_pct_start=0.15,
    copilot_pct_end=0.75,
    pipeline_failure_pct_start=0.35,
    pipeline_failure_pct_end=0.08,
)


# Pipeline stage steps: one per stage type, names match all 6 LIKE filters
_STAGE_STEPS = [
    ("build",      "build-and-test"),
    ("security",   "security-scan"),
    ("quality",    "sonar-quality"),
    ("test",       "qa-tests"),
    ("deploy",     "deploy-staging"),
    ("production", "production-release"),
]


_JIRA_ISSUE_TYPES = ["Story", "Task", "Bug"]
_JIRA_PRIORITIES  = ["Highest", "High", "Medium", "Low"]

_AUTHORS = [
    "alice.dev@demo.io",
    "bob.eng@demo.io",
    "carol.coder@demo.io",
    "david.devops@demo.io",
    "eve.engineer@demo.io",
]

_FEATURES = [
    "pagination", "filter caching", "auth retry", "metrics export",
    "schema migration", "rate limiting", "retry backoff", "structured logging",
    "input validation", "telemetry", "config reload", "circuit breaker",
]
_AREAS = [
    "dashboard", "API", "data pipeline", "auth service",
    "ingestion job", "report builder", "scheduler", "cache layer",
]


# ── SQL literal helpers ───────────────────────────────────────────────────────

def _q(s) -> str:
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def _ts(t: Optional[datetime]) -> str:
    if t is None:
        return "NULL"
    return f"TIMESTAMP '{t.isoformat(sep=' ')}'"


def _date(d: Optional[date]) -> str:
    if d is None:
        return "NULL"
    return f"DATE '{d.isoformat()}'"


def _i(n) -> str:
    return "NULL" if n is None else str(int(n))


def _f(n) -> str:
    return "NULL" if n is None else f"{float(n):.2f}"


# Column order matches CREATE TABLE — keep these in sync
_COLUMNS = (
    "sbg, gbe, offering, jira_sprint, org_name, project_name, jira_project, "
    "ticket_key, jira_issue_type, jira_summary, jira_priority, jira_severity, "
    "jira_status, jira_resolution, jira_assignee, jira_story_points, jira_issue_link, "
    "commit_id, commit_date, commit_timestamp, commit_author, commit_github_login, "
    "commit_title, copilot_commit_flag, commit_lines_added, commit_lines_removed, "
    "commit_lines_modified, is_merge_commit, is_bot_commit, "
    "pr_id, pr_title, pr_url, pr_state, pr_merged, pr_user_name, "
    "pr_created_datetime, pr_merged_datetime, pr_source_branch, pr_target_branch, "
    "pipeline_id, pipeline_name, pipeline_status, pipeline_event_type, "
    "pipeline_started_at, pipeline_finished_at, "
    "pipeline_step_name, pipeline_step_type, pipeline_step_status, pipeline_step_conclusion, "
    "pipeline_branch, pipeline_commit_sha, "
    "copilot_active_days, copilot_total_interactions, copilot_code_generations, "
    "copilot_code_acceptances, copilot_acceptance_rate_pct, copilot_loc_suggested, "
    "copilot_loc_added, copilot_used_chat, copilot_used_agent"
)


# ── Random helpers ────────────────────────────────────────────────────────────

def _rng_for(*parts) -> random.Random:
    """Stable RNG seeded by string parts — same inputs always produce the same output."""
    seed = int(hashlib.md5(":".join(str(p) for p in parts).encode()).hexdigest()[:8], 16)
    return random.Random(seed)


def _commit_sha(rng: random.Random) -> str:
    return "".join(rng.choices("0123456789abcdef", k=40))


# ── Per-row builders ──────────────────────────────────────────────────────────

def _build_jira(ticket_key: str, t: float, rng: random.Random) -> dict:
    issue_type = rng.choice(_JIRA_ISSUE_TYPES)

    # Older tickets (t low) more likely Done; recent tickets (t high) mixed.
    if t > 0.85:
        status_pool = ["Done", "Done", "In Review", "In Review", "In Progress", "In Progress", "To Do"]
    else:
        status_pool = ["Done"] * 7 + ["In Review", "In Progress", "To Do"]
    status = rng.choice(status_pool)
    resolution = "Done" if status == "Done" else None

    summary_template = {
        "Story": "Implement {feature} for {area}",
        "Task":  "Refactor {area} to support {feature}",
        "Bug":   "Fix {feature} regression in {area}",
    }[issue_type]
    summary = summary_template.format(feature=rng.choice(_FEATURES), area=rng.choice(_AREAS))

    return {
        "type":         issue_type,
        "summary":      summary,
        "priority":     rng.choices(_JIRA_PRIORITIES, weights=[1, 3, 5, 2])[0],
        "severity":     rng.choice(["Minor", "Major", "Critical"]) if issue_type == "Bug" else None,
        "status":       status,
        "resolution":   resolution,
        "assignee":     rng.choice(_AUTHORS),
        "story_points": rng.choice([1, 2, 3, 5, 8, 13]),
        "link":         f"https://jira.demo.io/browse/{ticket_key}",
    }


def _build_commit(ticket_key: str, ticket_date: date, idx: int,
                  copilot_rate: float, rng: random.Random) -> dict:
    commit_dt = datetime.combine(ticket_date, datetime.min.time()) + timedelta(hours=9 + idx * 2)
    author = rng.choice(_AUTHORS)
    login = author.split("@")[0].replace(".", "-")
    is_copilot = rng.random() < copilot_rate

    return {
        "id":             _commit_sha(rng),
        "date":           ticket_date,
        "ts":             commit_dt,
        "author":         author,
        "login":          login,
        "title":          f"{ticket_key}: commit {idx + 1}",
        "copilot_flag":   "Y" if is_copilot else None,
        "lines_added":    rng.randint(5, 200),
        "lines_removed":  rng.randint(0, 80),
        "lines_modified": rng.randint(0, 50),
        "is_merge":       "false",
        "is_bot":         "false",
    }


def _build_pr(cfg: OrgConfig, ticket_key: str, ticket_date: date, idx: int,
              jira: dict, rng: random.Random) -> dict:
    created = datetime.combine(ticket_date, datetime.min.time()) + timedelta(hours=14 + idx * 3)

    if jira["status"] == "Done":
        state, merged_flag = "merged", "true"
        merged_dt = created + timedelta(hours=rng.randint(2, 48))
    elif rng.random() < 0.7:
        state, merged_flag, merged_dt = "open", "false", None
    else:
        state, merged_flag, merged_dt = "closed", "false", None

    pr_num = rng.randint(1000, 9999)
    return {
        "id":         f"{cfg.project_name}#{pr_num}",
        "title":      f"{ticket_key}: {jira['summary']}",
        "url":        f"{cfg.project_url}/pull/{pr_num}",
        "state":      state,
        "merged":     merged_flag,
        "author":     rng.choice(_AUTHORS),
        "created":    created,
        "merged_dt":  merged_dt,
        "source":     f"feature/{ticket_key.lower()}",
        "target":     "main",
    }


def _build_pipeline_step(cfg: OrgConfig, ticket_key: str, ticket_date: date,
                         run_idx: int, step_type: str, step_name: str,
                         failure_rate: float, rng: random.Random) -> dict:
    started  = datetime.combine(ticket_date + timedelta(days=run_idx),
                                datetime.min.time()) + timedelta(hours=10 + run_idx * 4)
    finished = started + timedelta(minutes=20 + run_idx * 10)
    conclusion = "failure" if rng.random() < failure_rate else "success"

    return {
        "id":              f"{cfg.org_name}-{ticket_key}-pipeline-{run_idx + 1}",
        "name":            f"{cfg.project_name}-pipeline",
        "status":          "completed",
        "event_type":      "push",
        "started":         started,
        "finished":        finished,
        "step_name":       step_name,
        "step_type":       step_type,
        "step_status":     "completed",
        "step_conclusion": conclusion,
        "branch":          "main",
        "commit_sha":      _commit_sha(rng),
    }


def _build_copilot_rollup(copilot_rate: float, rng: random.Random) -> Optional[dict]:
    """Per-row author Copilot stats — populated for ~half the rows weighted by adoption."""
    if rng.random() > copilot_rate * 1.5:
        return None
    return {
        "active_days":         rng.randint(5, 22),
        "total_interactions":  rng.randint(50, 500),
        "code_generations":    rng.randint(20, 200),
        "code_acceptances":    rng.randint(10, 100),
        "acceptance_rate_pct": round(rng.uniform(40, 75), 2),
        "loc_suggested":       rng.randint(100, 1500),
        "loc_added":           rng.randint(50, 800),
        "used_chat":           "true" if rng.random() < 0.6 else "false",
        "used_agent":          "true" if rng.random() < 0.3 else "false",
    }


# ── Row assembly ──────────────────────────────────────────────────────────────

def _row(cfg: OrgConfig, sprint: str, ticket_key: str, jira: dict,
         *, commit=None, pr=None, pipeline=None, copilot=None) -> str:
    c  = commit   or {}
    p  = pr       or {}
    pl = pipeline or {}
    cp = copilot  or {}

    vals = [
        # Hierarchy / scope
        _q(cfg.sbg), _q(cfg.gbe), _q(cfg.offering), _q(sprint),
        _q(cfg.org_name), _q(cfg.project_name), _q(cfg.jira_project),
        # Jira
        _q(ticket_key), _q(jira["type"]), _q(jira["summary"]),
        _q(jira["priority"]), _q(jira.get("severity")), _q(jira["status"]),
        _q(jira.get("resolution")), _q(jira["assignee"]),
        _i(jira.get("story_points")), _q(jira["link"]),
        # Commit
        _q(c.get("id")), _date(c.get("date")), _ts(c.get("ts")),
        _q(c.get("author")), _q(c.get("login")), _q(c.get("title")),
        _q(c.get("copilot_flag")),
        _i(c.get("lines_added")), _i(c.get("lines_removed")), _i(c.get("lines_modified")),
        _q(c.get("is_merge")), _q(c.get("is_bot")),
        # PR
        _q(p.get("id")), _q(p.get("title")), _q(p.get("url")), _q(p.get("state")),
        _q(p.get("merged")), _q(p.get("author")),
        _ts(p.get("created")), _ts(p.get("merged_dt")),
        _q(p.get("source")), _q(p.get("target")),
        # Pipeline
        _q(pl.get("id")), _q(pl.get("name")), _q(pl.get("status")), _q(pl.get("event_type")),
        _ts(pl.get("started")), _ts(pl.get("finished")),
        _q(pl.get("step_name")), _q(pl.get("step_type")),
        _q(pl.get("step_status")), _q(pl.get("step_conclusion")),
        _q(pl.get("branch")), _q(pl.get("commit_sha")),
        # Copilot rollup
        _i(cp.get("active_days")), _i(cp.get("total_interactions")),
        _i(cp.get("code_generations")), _i(cp.get("code_acceptances")),
        _f(cp.get("acceptance_rate_pct")),
        _i(cp.get("loc_suggested")), _i(cp.get("loc_added")),
        _q(cp.get("used_chat")), _q(cp.get("used_agent")),
    ]
    return "(" + ", ".join(vals) + ")"


# ── Position helpers ──────────────────────────────────────────────────────────

def _ticket_t(cfg: OrgConfig, i: int) -> float:
    """Position 0..1 of the i-th ticket. Inflection orgs distribute pre/post around 0.5."""
    if not cfg.inflection:
        return i / max(cfg.ticket_count - 1, 1)
    if i < cfg.pre_count:
        return (i / max(cfg.pre_count - 1, 1)) * 0.5
    return 0.5 + ((i - cfg.pre_count) / max(cfg.post_count - 1, 1)) * 0.5


def _ticket_date(t: float, story_start: date, story_end: date) -> date:
    span = (story_end - story_start).days
    return story_start + timedelta(days=int(t * span))


def _sprint_for(d: date, story_start: date, cfg: OrgConfig) -> str:
    days_in = (d - story_start).days
    sprint_num = min(cfg.sprint_count, max(1, days_in // cfg.sprint_length_days + 1))
    return f"{cfg.sprint_prefix} {sprint_num}"


# ── Public entrypoint ─────────────────────────────────────────────────────────

def generate(catalog: str, cfg: OrgConfig, story: dict, batch_size: int = 100) -> list[str]:
    """Return a list of INSERT statements for the given org config + story window."""
    story_start = date.fromisoformat(story["start_date"])
    story_end   = date.fromisoformat(story["end_date"])

    all_values: list[str] = []

    for i in range(cfg.ticket_count):
        t = _ticket_t(cfg, i)
        ticket_date = _ticket_date(t, story_start, story_end)
        ticket_key  = f"{cfg.jira_project}-{cfg.ticket_id_start + i}"
        sprint      = _sprint_for(ticket_date, story_start, cfg)

        rng = _rng_for(ticket_key)
        jira = _build_jira(ticket_key, t, rng)
        copilot_rate = lerp(cfg.copilot_pct_start, cfg.copilot_pct_end, t)
        failure_rate = lerp(cfg.pipeline_failure_pct_start, cfg.pipeline_failure_pct_end, t)
        copilot = _build_copilot_rollup(copilot_rate, rng)

        # Commit rows
        for ci in range(rng.randint(2, 4)):
            commit = _build_commit(ticket_key, ticket_date, ci, copilot_rate, rng)
            all_values.append(_row(cfg, sprint, ticket_key, jira, commit=commit, copilot=copilot))

        # PR rows
        for pi in range(rng.randint(1, 2)):
            pr = _build_pr(cfg, ticket_key, ticket_date, pi, jira, rng)
            all_values.append(_row(cfg, sprint, ticket_key, jira, pr=pr, copilot=copilot))

        # Pipeline rows: 2 runs × 6 step rows each
        for run_idx in range(2):
            for step_type, step_name in _STAGE_STEPS:
                pipeline = _build_pipeline_step(
                    cfg, ticket_key, ticket_date, run_idx, step_type, step_name, failure_rate, rng,
                )
                all_values.append(_row(cfg, sprint, ticket_key, jira, pipeline=pipeline, copilot=copilot))

    # Chunk into batched INSERTs to avoid oversized statements
    statements: list[str] = []
    for chunk_start in range(0, len(all_values), batch_size):
        chunk = all_values[chunk_start : chunk_start + batch_size]
        statements.append(
            f"INSERT INTO {catalog}.{TABLE} ({_COLUMNS}) VALUES\n"
            + ",\n".join(chunk)
        )
    return statements
