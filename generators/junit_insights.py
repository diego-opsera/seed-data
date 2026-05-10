"""
Generator for source_to_stage.junit_test_suite_report.

Drives the JUnit Insights widget on the Code Reliability dashboard. SQL
(junit_overview_data.sql) joins git_url to filter_groups.project_url
(already wired for our orgs from DORA work). Per time bucket the widget
sums passed/failed/errored/skipped test counts.

Per project: weekly test runs over the last ~90 days. Counts shift by
org phase:
  Acme    : ~95% pass baseline; March/Nov spikes drop to ~70% pass
  Meridian: pre-Opsera ~70% pass → post-Opsera ~95% pass

Scope tag: service_principal ∈ {'seed-data', 'seed-data-meridian'}.
(junit_test_suite_report has no record_inserted_by column;
service_principal is the closest equivalent for delete scoping.)
"""
from __future__ import annotations

import hashlib
import random
from datetime import date, datetime, timedelta

from .utils import _sql_val

TABLE = "junit_test_suite_report"

INSERT_SQL = """\
INSERT INTO {catalog}.source_to_stage.junit_test_suite_report
  (_id, test_name, total_time_in_seconds,
   total_tests, errored_tests, skipped_tests, failed_tests, passed_tests,
   customer_id, pipeline_id, pipeline_name, step_id, step_name,
   pipeline_tags, repository, git_url, git_branch, commit_id, run_count,
   _class, created_at, status, service_principal,
   source_record_insert_datetime)
VALUES
{values};"""


def _is_meridian(org_name: str) -> bool:
    return org_name == "demo-meridian"


def _stable_oid(*parts) -> str:
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:24]


def _ts_lit(dt: datetime) -> str:
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def _pass_rate(org_name: str, scan_day: date, story: dict, start: date, end: date) -> float:
    """Per-phase pass rate."""
    if _is_meridian(org_name):
        total = max((end - start).days, 1)
        t = max(0.0, min(1.0, (scan_day - start).days / total))
        # Pre-Opsera ~70% → post-Opsera ~95%
        if t < 0.5:
            return 0.70
        local_t = (t - 0.5) / 0.5
        return 0.70 + 0.25 * local_t

    if story.get("security_spikes", False):
        if date(2026, 3, 2)  <= scan_day <= date(2026, 3, 16):
            return 0.65
        if date(2025, 11, 17) <= scan_day <= date(2025, 11, 24):
            return 0.78
    return 0.95


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    scope = "seed-data-meridian" if _is_meridian(org_name) else "seed-data"

    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    repos = entities.get("repos", [])
    if not repos:
        return []

    # Weekly scans over the last 90 days, snapped to Mondays
    scan_days = []
    d = end - timedelta(days=90)
    while d.weekday() != 0:
        d += timedelta(days=1)
    while d <= end:
        scan_days.append(d)
        d += timedelta(days=7)

    value_lines = []

    for repo in repos:
        full_name  = repo["name"]                       # demo-acme-direct/backend
        repository = full_name.split("/", 1)[-1]
        # Match what's wired in filter_values_unity.project_url (no .git)
        git_url    = repo.get("html_url", f"https://github.com/{full_name}")
        if git_url.endswith(".git"):
            git_url = git_url[:-4]

        pipeline_id   = _stable_oid("pipeline", org_name, repository)
        pipeline_name = f"{org_name}/{repository}"
        step_id       = _stable_oid("step", repository, "unit-test")
        run_count     = 100

        for scan_day in scan_days:
            run_count += 1
            rng = random.Random(hash((org_name, repository, scan_day.isoformat(), "junit")) % (2**31))
            scan_dt = datetime(scan_day.year, scan_day.month, scan_day.day,
                               rng.randint(2, 18), rng.randint(0, 59), rng.randint(0, 59),
                               rng.randint(0, 999999))

            total_tests   = rng.randint(80, 220)
            pass_rate     = _pass_rate(org_name, scan_day, story, start, end)
            passed        = int(total_tests * pass_rate)
            remaining     = total_tests - passed
            # Split remaining across failed / errored / skipped (60/25/15)
            failed        = int(remaining * 0.60)
            errored       = int(remaining * 0.25)
            skipped       = remaining - failed - errored
            total_time    = round(total_tests * rng.uniform(0.05, 0.25), 3)
            commit_id     = hashlib.sha1(f"{repository}|{scan_day}|{run_count}".encode()).hexdigest()
            test_id       = _stable_oid("junit", org_name, repository, scan_day.isoformat())
            test_name     = f"com.{repository.replace('-', '.')}.UnitTests"
            status        = "Successful" if (failed == 0 and errored == 0) else "Failed"

            tags_array = f"ARRAY({_sql_val(f'unit-test_{step_id}')})"

            value_lines.append(
                "  ("
                f"{_sql_val(test_id)}, {_sql_val(test_name)}, {total_time}, "
                f"{total_tests}, {errored}, {skipped}, {failed}, {passed}, "
                f"{_sql_val(scope + '-customer')}, "
                f"{_sql_val(pipeline_id)}, {_sql_val(pipeline_name)}, "
                f"{_sql_val(step_id)}, {_sql_val('unit test')}, "
                f"{tags_array}, {_sql_val(repository)}, {_sql_val(git_url)}, "
                f"{_sql_val('main')}, {_sql_val(commit_id)}, {_sql_val(str(run_count))}, "
                f"{_sql_val('com.opsera.pipeline.repository.JUnitTestSuite')}, "
                f"{_ts_lit(scan_dt)}, {_sql_val(status)}, {_sql_val(scope)}, "
                f"{_ts_lit(scan_dt)}"
                ")"
            )

    if not value_lines:
        return []

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
