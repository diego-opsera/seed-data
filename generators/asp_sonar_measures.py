"""
Generator for source_to_stage.raw_sonar_metric_split_data_branchwise.

base_datasets.asp_sonar_measures is a VIEW over this table (with a LEFT JOIN
to raw_sonar_project_branch_list to decorate sonar_instance + project_tags).
We INSERT into the underlying table; the view surfaces the rows.

Pairs with asp_sonar_issues.py via shared scan timestamps from
asp_sonar_issues.scan_calendar() — the JOIN in sonar_ratings_overview.sql
is on (org_name, project, branch, source_record_insert_datetime).

Drives:
  - Sonar Ratings Overview      (Reliability/Security/Maintainability ratings)
  - Coverage Trend / Coverage Table
  - Defect Density (Sonarqube)  Overview

Story arcs:
  Acme    — coverage steady ~75-80%, ratings A/B, with March 2026 + Nov 2025
            spike weeks where security rating drops + coverage dips.
  Meridian— coverage 40% → 75% across the year, ratings C/D pre-Opsera
            → A/B post-Opsera (inflection at t=0.5).

Scope tag for delete: record_inserted_by ∈ {'seed-data', 'seed-data-meridian'}.
"""
from __future__ import annotations

import random
from datetime import date, datetime

from .utils import _sql_val, lerp
from . import asp_sonar_issues  # for scan_calendar()

TABLE = "raw_sonar_metric_split_data_branchwise"

INSERT_SQL = """\
INSERT INTO {catalog}.source_to_stage.raw_sonar_metric_split_data_branchwise
  (org_name, project_name, branch, component_name, last_analysis_date,
   tests_value, comment_lines_value,
   cognitive_complexity_value, cyclomatic_complexity_value,
   project_coverage_value, project_lines_to_cover_value,
   project_uncovered_lines_value, project_line_coverage_value,
   project_coverage_bestvalue, project_lines_to_cover_bestvalue,
   project_uncovered_lines_bestvalue, project_line_coverage_bestvalue,
   project_bugs_value, project_bugs_bestvalue,
   project_ncloc_value, project_ncloc_bestvalue,
   project_sqale_index_value, project_sqale_index_bestvalue,
   project_sqale_rating_value, project_sqale_rating_bestvalue,
   project_reliability_rating_value, project_reliability_rating_bestvalue,
   project_security_rating_value,
   project_duplicated_lines_value, project_duplicated_lines_bestvalue,
   new_lines_to_cover_index, new_lines_to_cover_value,
   new_uncovered_lines_value, new_uncovered_lines_index, new_uncovered_lines_bestvalue,
   new_coverage_value, new_coverage_index, new_coverage_bestvalue,
   new_line_coverage_value, new_line_coverage_index, new_line_coverage_bestvalue,
   quality_gate_name, quality_gate_status,
   data_source,
   source_record_insert_datetime, source_record_update_datetime,
   record_inserted_by)
VALUES
{values};"""


def _is_meridian(org_name: str) -> bool:
    return org_name == "demo-meridian"


def _phase_t(scan_day: date, start: date, end: date) -> float:
    total = max((end - start).days, 1)
    return max(0.0, min(1.0, (scan_day - start).days / total))


def _ts_lit(dt: datetime) -> str:
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def _coverage_pct(org_name: str, scan_day: date, story: dict, start: date, end: date,
                  rng: random.Random) -> float:
    """Return overall project coverage value as a percentage (0..100)."""
    if _is_meridian(org_name):
        # Pre-Opsera 40% → 65%, post-Opsera 65% → 80%
        t = _phase_t(scan_day, start, end)
        if t < 0.5:
            base = lerp(40.0, 65.0, t / 0.5)
        else:
            base = lerp(65.0, 80.0, (t - 0.5) / 0.5)
    else:
        base = 76.0
        # Spike windows dip coverage as devs rush hotfixes (less test coverage on patches).
        # Windows come from story["events"] (anchored to today).
        if story.get("security_spikes", False):
            events = story.get("events", {})
            if scan_day in events.get("acme_spike_broad", frozenset()):
                base = 56.0
            elif scan_day in events.get("acme_secondary_spike", frozenset()):
                base = 66.0
    return round(base + rng.uniform(-2.0, 2.0), 1)


def _rating_for_phase(org_name: str, scan_day: date, story: dict, start: date, end: date,
                      category: str) -> str:
    """Return a Sonar rating value as a string ('1.0' .. '5.0').
       category ∈ {'reliability', 'security', 'maintainability'}."""
    if _is_meridian(org_name):
        t = _phase_t(scan_day, start, end)
        if t < 0.5:
            # Pre-Opsera: C/D ratings (3.0-4.0)
            return "4.0" if category == "security" else "3.0"
        else:
            # Post-Opsera: gradual improvement to A/B
            local_t = (t - 0.5) / 0.5
            if local_t < 0.3:
                return "3.0"
            if local_t < 0.6:
                return "2.0"
            return "1.0"

    # Acme baseline: A/B; spikes elevate security rating
    events = story.get("events", {})
    in_primary  = scan_day in events.get("acme_spike_broad", frozenset())
    in_secondary = scan_day in events.get("acme_secondary_spike", frozenset())
    if story.get("security_spikes", False) and (in_primary or in_secondary):
        if category == "security":
            return "4.0" if in_primary else "3.0"
        return "2.0"
    return "2.0" if category == "maintainability" else "1.0"


def _quality_gate_status(reliability: str, security: str, maintainability: str) -> str:
    """Sonar 'OK' if all three ratings are A or B (1.0 / 2.0); 'ERROR' otherwise."""
    if all(r in ("1.0", "2.0") for r in (reliability, security, maintainability)):
        return "OK"
    return "ERROR"


def _ncloc_for(scan_day: date, start: date, end: date, project: str,
               org_name: str, rng: random.Random) -> int:
    """Lines of code grow modestly over time, anchored per project."""
    base = abs(hash(("ncloc-base", project))) % 15000 + 8000
    if _is_meridian(org_name):
        base = max(4000, base // 2)   # Meridian projects are smaller
    growth = int((scan_day - start).days * (12 if _is_meridian(org_name) else 25))
    return base + growth + rng.randint(-200, 200)


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    record_inserted_by = "seed-data-meridian" if _is_meridian(org_name) else "seed-data"

    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    scans = asp_sonar_issues.scan_calendar(entities, story)
    if not scans:
        return []

    value_lines = []

    for project, branch, scan_dt in scans:
        rng = random.Random(hash((org_name, project, scan_dt.isoformat(), "measures")) % (2**31))

        scan_day = scan_dt.date()

        coverage    = _coverage_pct(org_name, scan_day, story, start, end, rng)
        ncloc       = _ncloc_for(scan_day, start, end, project, org_name, rng)
        lines_to_cover    = max(1, int(ncloc * 0.85))   # ~85% of LOC are coverable
        uncovered_lines   = int(lines_to_cover * (1.0 - coverage / 100.0))
        line_coverage     = round(((lines_to_cover - uncovered_lines) / lines_to_cover) * 100.0, 1)

        # New code (last analysis period) — small slice
        new_lines_to_cover  = rng.randint(20, 200)
        new_uncovered       = int(new_lines_to_cover * (1.0 - coverage / 100.0))
        new_coverage        = round(((new_lines_to_cover - new_uncovered) / new_lines_to_cover) * 100.0, 1) \
                              if new_lines_to_cover else 0.0

        # Ratings
        reliability   = _rating_for_phase(org_name, scan_day, story, start, end, "reliability")
        security      = _rating_for_phase(org_name, scan_day, story, start, end, "security")
        maintainability = _rating_for_phase(org_name, scan_day, story, start, end, "maintainability")

        # Bugs / tech debt — derive from rating buckets
        bugs_count = {
            "1.0": rng.randint(0, 2),
            "2.0": rng.randint(3, 8),
            "3.0": rng.randint(10, 18),
            "4.0": rng.randint(20, 35),
            "5.0": rng.randint(40, 60),
        }[reliability]
        sqale_index = {
            "1.0": rng.randint(0, 60),
            "2.0": rng.randint(60, 240),
            "3.0": rng.randint(240, 720),
            "4.0": rng.randint(720, 2400),
            "5.0": rng.randint(2400, 7200),
        }[maintainability]
        cognitive_complexity = rng.randint(50, 400)
        cyclomatic_complexity = rng.randint(80, 600)
        tests = rng.randint(20, 250)
        comment_lines = max(1, int(ncloc * rng.uniform(0.05, 0.20)))
        duplicated_lines = int(ncloc * rng.uniform(0.005, 0.040))

        gate_status = _quality_gate_status(reliability, security, maintainability)

        # `_value` columns are STRINGs in the schema — emit as quoted strings.
        value_lines.append(
            "  ("
            f"{_sql_val(org_name)}, {_sql_val(project)}, {_sql_val(branch)}, {_sql_val(project)}, "
            f"{_ts_lit(scan_dt)}, "
            f"{_sql_val(str(tests))}, {_sql_val(str(comment_lines))}, "
            f"{_sql_val(str(cognitive_complexity))}, {_sql_val(str(cyclomatic_complexity))}, "
            f"{_sql_val(str(coverage))}, {_sql_val(str(lines_to_cover))}, "
            f"{_sql_val(str(uncovered_lines))}, {_sql_val(str(line_coverage))}, "
            f"FALSE, NULL, FALSE, FALSE, "
            f"{_sql_val(str(bugs_count))}, "
            f"{'TRUE' if bugs_count == 0 else 'FALSE'}, "
            f"{_sql_val(str(ncloc))}, NULL, "
            f"{_sql_val(str(sqale_index))}, FALSE, "
            f"{_sql_val(maintainability)}, {'TRUE' if maintainability == '1.0' else 'FALSE'}, "
            f"{_sql_val(reliability)}, {'TRUE' if reliability == '1.0' else 'FALSE'}, "
            f"{_sql_val(security)}, "
            f"{_sql_val(str(duplicated_lines))}, FALSE, "
            f"{_sql_val('1')}, {_sql_val(str(new_lines_to_cover))}, "
            f"{_sql_val(str(new_uncovered))}, {_sql_val('1')}, "
            f"{'TRUE' if new_uncovered == 0 else 'FALSE'}, "
            f"{_sql_val(str(new_coverage))}, {_sql_val('1')}, "
            f"{'FALSE' if new_coverage < 100 else 'TRUE'}, "
            f"{_sql_val(str(new_coverage))}, {_sql_val('1')}, "
            f"{'FALSE' if new_coverage < 100 else 'TRUE'}, "
            f"{_sql_val('Sonar way')}, {_sql_val(gate_status)}, "
            f"{_sql_val('sonar api')}, "
            f"{_ts_lit(scan_dt)}, {_ts_lit(scan_dt)}, "
            f"{_sql_val(record_inserted_by)}"
            ")"
        )

    if not value_lines:
        return []

    chunk = 200
    statements = []
    for i in range(0, len(value_lines), chunk):
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines[i:i + chunk])))
    return statements
