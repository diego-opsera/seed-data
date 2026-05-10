"""
Generator for source_to_stage.raw_mongo_transformed_data_gitscraper +
raw_mongo_transformed_data_gitscraper_issues.

Drives the Git Custodian widget on the Code Reliability dashboard. The SQL
(gc_overview.sql + gc_trend.sql) joins:
   raw_mongo_transformed_data_gitscraper s
   ⋈ filter_groups (on pipelineName / branch / pipeline_tags)
   ⋈ raw_mongo_transformed_data_gitscraper_issues i (on s._id = i._id)

Per project: weekly scans across the full story window, each with a small
number of secret-scanning issues. Issue counts trend DOWN over time so the
widget's "current vs previous" period diff shows resolution progress.

Story arcs:
  Acme    : ~3-5 issues/scan baseline, +5 during March 2026 + Nov 2025 spikes
  Meridian: pre-Opsera 5-8 issues/scan → post-Opsera 0-2 (improvement story)

Scope tag: record_inserted_by ∈ {'seed-data', 'seed-data-meridian'}.
"""
from __future__ import annotations

import hashlib
import random
from datetime import date, datetime, timedelta

from .utils import _sql_val

TABLE = "raw_mongo_transformed_data_gitscraper"

INSERT_SCANS_SQL = """\
INSERT INTO {catalog}.source_to_stage.raw_mongo_transformed_data_gitscraper
  (_id, giturl, branch, pipelineName, pipelineId, runCount, repository,
   totalIssues, activityDate, tags, record_inserted_by, record_insert_datetime)
VALUES
{values};"""

INSERT_ISSUES_SQL = """\
INSERT INTO {catalog}.source_to_stage.raw_mongo_transformed_data_gitscraper_issues
  (_id, lineNumber, path, reason, severity, status, author, email,
   record_inserted_by, record_insert_datetime)
VALUES
{values};"""

_REASONS = [
    ("AWS Access Key",         "Critical"),
    ("Stripe API Key",         "Critical"),
    ("Generic API Key",        "High"),
    ("Slack Token",            "High"),
    ("GitHub Personal Token",  "High"),
    ("Database Password",      "High"),
    ("Private SSH Key",        "Critical"),
    ("Hardcoded Secret",       "Medium"),
    ("Email Address Exposure", "Low"),
    ("URL with Credentials",   "Medium"),
]

_PATHS = [
    "src/config/secrets.yml",
    ".env",
    "config/db.yml",
    "scripts/deploy.sh",
    "src/utils/api-client.js",
    "test/fixtures/sample.json",
    "deploy/k8s-secrets.yaml",
    "docker-compose.yml",
    "src/main/resources/application.properties",
    "infra/terraform.tfvars",
]


def _is_meridian(org_name: str) -> bool:
    return org_name == "demo-meridian"


def _stable_oid(*parts) -> str:
    """24-char ObjectId-shaped hash so _id matches MongoDB convention."""
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:24]


def _ts_lit(dt: datetime) -> str:
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def _tags_array(pipeline_name: str, branch: str) -> str:
    """Match the SQL's: arrays_overlap(transform(tags, e-> concat(e.type,':',e.value)), f.pipeline_tags)
       So tags is array of struct<type,value>; concat'd values like 'pipeline:my-pipeline' must be
       discoverable by the filter wiring's pipeline_tags array."""
    return (
        "ARRAY("
        f"NAMED_STRUCT('type', 'pipeline', 'value', {_sql_val(pipeline_name)}),"
        f"NAMED_STRUCT('type', 'branch',   'value', {_sql_val(branch)})"
        ")"
    )


def _issues_per_scan(org_name: str, scan_day: date, story: dict, start: date, end: date,
                     rng: random.Random) -> int:
    if _is_meridian(org_name):
        # Pre-Opsera 5-8, post-Opsera 0-2
        total = max((end - start).days, 1)
        t = max(0.0, min(1.0, (scan_day - start).days / total))
        if t < 0.5:
            return rng.randint(5, 8)
        local_t = (t - 0.5) / 0.5
        # Decay 5 → 0 across post phase
        cap = max(0, int(round(5 * (1 - local_t))))
        return rng.randint(0, max(1, cap))

    base = rng.randint(2, 5)
    if story.get("security_spikes", False):
        events = story.get("events", {})
        if scan_day in events.get("acme_spike_broad", frozenset()):
            return base + rng.randint(4, 6)
        if scan_day in events.get("acme_secondary_spike", frozenset()):
            return base + rng.randint(2, 4)
    return base


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    record_inserted_by = "seed-data-meridian" if _is_meridian(org_name) else "seed-data"

    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    repos = entities.get("repos", [])
    if not repos:
        return []

    user_logins = [u.get("login") for u in entities.get("users", []) if u.get("login")]
    if not user_logins:
        user_logins = [f"{org_name}-bot"]

    # Weekly scans across the FULL story window (rolling 1-year ending today),
    # snapped to Mondays. Lets the trend chart show the November spike +
    # Meridian's pre/post-Opsera inflection.
    scan_days = []
    d = start
    while d.weekday() != 0:
        d += timedelta(days=1)
    while d <= end:
        scan_days.append(d)
        d += timedelta(days=7)

    scan_value_lines = []
    issue_value_lines = []

    for repo in repos:
        full_name = repo["name"]                        # e.g. demo-acme-direct/backend
        giturl    = repo.get("html_url", f"https://github.com/{full_name}.git")
        if not giturl.endswith(".git"):
            giturl = giturl + ".git"
        repository = full_name.split("/", 1)[-1]        # e.g. backend
        branch     = "main"
        pipeline_name = f"{repository}-pipeline"
        pipeline_id   = _stable_oid("pipeline", org_name, repository)

        run_count = 100
        for scan_day in scan_days:
            run_count += 1
            rng = random.Random(hash((org_name, repository, scan_day.isoformat())) % (2**31))
            scan_dt = datetime(scan_day.year, scan_day.month, scan_day.day,
                               rng.randint(2, 18), rng.randint(0, 59), rng.randint(0, 59))

            n_issues = _issues_per_scan(org_name, scan_day, story, start, end, rng)
            scan_id = _stable_oid("scan", org_name, repository, scan_day.isoformat())

            scan_value_lines.append(
                "  ("
                f"{_sql_val(scan_id)}, {_sql_val(giturl)}, {_sql_val(branch)}, "
                f"{_sql_val(pipeline_name)}, {_sql_val(pipeline_id)}, {run_count}, "
                f"{_sql_val(repository)}, {n_issues}, {_ts_lit(scan_dt)}, "
                f"{_tags_array(pipeline_name, branch)}, "
                f"{_sql_val(record_inserted_by)}, {_ts_lit(scan_dt)}"
                ")"
            )

            for issue_seq in range(n_issues):
                issue_rng = random.Random(hash((scan_id, issue_seq)) % (2**31))
                reason, severity = issue_rng.choice(_REASONS)
                path = issue_rng.choice(_PATHS)
                line = issue_rng.randint(5, 250)
                author = issue_rng.choice(user_logins)
                email = f"{author}@example.com"
                status = "Open"

                issue_value_lines.append(
                    "  ("
                    f"{_sql_val(scan_id)}, {line}, {_sql_val(path)}, {_sql_val(reason)}, "
                    f"{_sql_val(severity)}, {_sql_val(status)}, "
                    f"{_sql_val(author)}, {_sql_val(email)}, "
                    f"{_sql_val(record_inserted_by)}, {_ts_lit(scan_dt)}"
                    ")"
                )

    statements = []
    if scan_value_lines:
        statements.append(INSERT_SCANS_SQL.format(catalog=catalog, values=",\n".join(scan_value_lines)))
    if issue_value_lines:
        # Chunk to keep individual INSERTs reasonable
        chunk = 500
        for i in range(0, len(issue_value_lines), chunk):
            statements.append(INSERT_ISSUES_SQL.format(
                catalog=catalog, values=",\n".join(issue_value_lines[i:i + chunk])
            ))
    return statements
