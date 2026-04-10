"""
generators/cr_meridian.py

Generates INSERT statements for transform_stage.trf_servicenow_change_requests
scoped to the Meridian Analytics data engineering team.

Story arc:
  Pre-Opsera: one Emergency/Normal CR per quarterly maintenance window
    (same months as dora_meridian's maintenance deploys: idx % 3 == 2).
    CR is opened at the start of the month, takes 2-4 weeks to resolve.
    No Opsera correlation — everything is manual.

  Post-Opsera: 1-2 Standard pre-approved CRs per week, 1-3 day resolution,
    ~60% carry an Opsera correlation_id.

Deletion scoped via issue_key LIKE 'meridian-seed-chg-%'.
Filter wired via assignment_groups = 'Meridian Data Engineering'.
"""
import random
from datetime import date, datetime, timedelta

from .utils import date_range, _sql_val
from .dora_meridian import _build_months, _phase_t

TABLE  = "trf_servicenow_change_requests"
SCHEMA = "transform_stage"

INSERT_SQL = """\
INSERT INTO {catalog}.transform_stage.trf_servicenow_change_requests
  (issue_key, issue_summary, issue_project,
   issue_status, issue_resolution_name,
   issue_start_date, issue_started_at, issue_resolved_at, issue_updated_at,
   correlation_id, itsm_source, scope, cr_class,
   issue_created_by, issue_updated_by, closed_by, assignee_name)
VALUES
{values};"""

ASSIGNMENT_GROUP = "Meridian Data Engineering"

_SUMMARIES_PRE = [
    "Promote data pipeline changes to production Databricks workspace",
    "Data platform schema migration to production",
    "Deploy notebook changes to production workspace",
    "Quarterly data ingestion pipeline upgrade",
    "Update Databricks cluster configuration for production",
    "Production workspace promotion: ETL refactor release",
    "Data platform release: batch job configuration update",
]

_SUMMARIES_POST = [
    "Opsera automated deployment: data-platform pipeline",
    "Standard change: deploy data-platform PR merge to production",
    "Pre-approved change: Databricks job update via Opsera",
    "Scheduled pipeline deployment via Opsera",
    "Standard change: notebook update to production workspace",
    "Opsera pipeline deploy: data ingestion update",
]

_STATUSES_DONE   = ["Resolved", "Closed"]
_STATUSES_FAILED = ["Closed"]
_STATUSES_OPEN   = ["In Progress", "Scheduled"]

_USERS = ["meridian-alice", "meridian-bob", "meridian-carol"]


def _opsera_correlation_id(seed: int) -> str:
    rng = random.Random(seed)
    pipeline_id = f"pipe{rng.randint(100, 999)}"
    run = rng.randint(1, 200)
    return f"opsera_{pipeline_id}_deploy_{run}"


def _ts(dt) -> str:
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'" if dt else "NULL"


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    months         = _build_months(start, end)
    total_months   = len(months)
    inflection_idx = total_months // 2

    cr_counter  = 1
    value_lines = []

    for idx, (yr, mo) in enumerate(months):
        t_phase  = _phase_t(idx, inflection_idx, total_months)
        mo_start = date(yr, mo, 1)
        mo_end   = (date(yr, mo + 1, 1) if mo < 12 else date(yr + 1, 1, 1)) - timedelta(days=1)
        mo_end   = min(mo_end, end)
        if mo_start > end:
            break

        if idx < inflection_idx:
            # Pre-Opsera: CR only in maintenance months (every 3rd month)
            if idx % 3 != 2:
                continue
            # 1-2 CRs per maintenance window, opened first week of month
            rng_m = random.Random(yr * 100 + mo + 77)
            n_crs = rng_m.randint(1, 2)
            for seq in range(n_crs):
                rng = random.Random(hash((yr, mo, seq, "cr-pre")) % (2**31))
                issue_key = f"meridian-seed-chg-{cr_counter:06d}"
                cr_counter += 1
                summary  = rng.choice(_SUMMARIES_PRE)
                creator  = rng.choice(_USERS)
                assignee = rng.choice(_USERS)
                # Open in first 5 business days of the month
                open_day = mo_start + timedelta(days=rng.randint(0, 7))
                open_day = min(open_day, mo_end)
                while open_day.weekday() >= 5:
                    open_day += timedelta(days=1)
                started_at = datetime(open_day.year, open_day.month, open_day.day,
                                      rng.randint(8, 12), rng.randint(0, 59))

                # Long resolution: 14-28 days (CAB approval + scheduling)
                resolve_days = rng.randint(14, 28)
                resolved_dt  = started_at + timedelta(days=resolve_days)

                scope    = rng.choice(["Emergency", "Normal"])
                cr_class = scope

                outcome = rng.random()
                if resolved_dt.date() > end or outcome >= 0.95:
                    resolution_name = None
                    status          = rng.choice(_STATUSES_OPEN)
                    resolved_dt     = None
                    closed_by       = None
                elif outcome < 0.80:
                    resolution_name = "Done"
                    status          = rng.choice(_STATUSES_DONE)
                    closed_by       = rng.choice(_USERS)
                else:
                    resolution_name = "Failed"
                    status          = rng.choice(_STATUSES_FAILED)
                    closed_by       = rng.choice(_USERS)

                updated_dt     = resolved_dt if resolved_dt else started_at + timedelta(hours=rng.randint(1, 4))
                correlation_id = None  # No Opsera pre-adoption

                value_lines.append(
                    f"  ({_sql_val(issue_key)}, {_sql_val(summary)}, {_sql_val(ASSIGNMENT_GROUP)}, "
                    f"{_sql_val(status)}, {_sql_val(resolution_name)}, "
                    f"DATE '{open_day.isoformat()}', {_ts(started_at)}, {_ts(resolved_dt)}, {_ts(updated_dt)}, "
                    f"{_sql_val(correlation_id)}, 'servicenow', {_sql_val(scope)}, {_sql_val(cr_class)}, "
                    f"{_sql_val(creator)}, {_sql_val(assignee)}, {_sql_val(closed_by)}, {_sql_val(assignee)})"
                )

        else:
            # Post-Opsera: 1-2 Standard CRs per week, iterate over business days
            bdays = []
            d = mo_start
            while d <= mo_end:
                if d.weekday() < 5:
                    bdays.append(d)
                d += timedelta(days=1)

            # Pick ~1.5 CRs/week — ~6/month
            rng_m = random.Random(yr * 100 + mo + 88)
            n_crs = rng_m.randint(4, 8)
            # Spread evenly across business days
            if len(bdays) == 0:
                continue
            chosen_days = sorted(rng_m.choices(bdays, k=n_crs))

            for seq, open_day in enumerate(chosen_days):
                rng = random.Random(hash((yr, mo, seq, "cr-post")) % (2**31))
                issue_key = f"meridian-seed-chg-{cr_counter:06d}"
                cr_counter += 1
                summary  = rng.choice(_SUMMARIES_POST)
                creator  = rng.choice(_USERS)
                assignee = rng.choice(_USERS)
                started_at = datetime(open_day.year, open_day.month, open_day.day,
                                      rng.randint(8, 16), rng.randint(0, 59))

                # Fast resolution: 1-3 days
                resolve_days = rng.randint(1, 3)
                resolved_dt  = started_at + timedelta(days=resolve_days)

                scope    = "Standard"
                cr_class = "Standard"

                is_opsera      = rng.random() < 0.60
                correlation_id = _opsera_correlation_id(hash((yr, mo, seq)) % (2**31)) if is_opsera else None

                outcome = rng.random()
                if resolved_dt.date() > end or outcome >= 0.95:
                    resolution_name = None
                    status          = rng.choice(_STATUSES_OPEN)
                    resolved_dt     = None
                    closed_by       = None
                elif outcome < 0.88:
                    resolution_name = "Done"
                    status          = rng.choice(_STATUSES_DONE)
                    closed_by       = rng.choice(_USERS)
                else:
                    resolution_name = "Failed"
                    status          = rng.choice(_STATUSES_FAILED)
                    closed_by       = rng.choice(_USERS)

                updated_dt = resolved_dt if resolved_dt else started_at + timedelta(hours=rng.randint(1, 4))

                value_lines.append(
                    f"  ({_sql_val(issue_key)}, {_sql_val(summary)}, {_sql_val(ASSIGNMENT_GROUP)}, "
                    f"{_sql_val(status)}, {_sql_val(resolution_name)}, "
                    f"DATE '{open_day.isoformat()}', {_ts(started_at)}, {_ts(resolved_dt)}, {_ts(updated_dt)}, "
                    f"{_sql_val(correlation_id)}, 'servicenow', {_sql_val(scope)}, {_sql_val(cr_class)}, "
                    f"{_sql_val(creator)}, {_sql_val(assignee)}, {_sql_val(closed_by)}, {_sql_val(assignee)})"
                )

    chunk_size = 500
    statements = []
    for i in range(0, len(value_lines), chunk_size):
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines[i:i + chunk_size])))
    return statements
