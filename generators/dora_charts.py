"""
generators/dora_charts.py

Generates INSERT statements for the DORA individual metric chart tables:
  base_datasets.pipeline_activities   — DF chart (GitHub deployment events)
  base_datasets.cfr_mttr_metric_data  — CFR and MTTR charts (Jira issue data)

Arc mirrors generators/dora.py (Apr 2025 → Mar 2026) so chart values are
consistent with the SDM maturity badge scores in consumption_layer.sdm.

Scoped for safe deletion by:
  pipeline_activities:   record_inserted_by = 'seed-data'
  cfr_mttr_metric_data:  record_inserted_by = 'seed-data'

Requires filter_values_unity entries (inserted via insert_filter_group.py):
  filter_name='project_url',  filter_values=['https://github.com/demo-acme/project_001.git']
  filter_name='project_name', filter_values=['Acme Platform']
"""

import random
from datetime import date, datetime, timedelta

from .utils import lerp, jitter

TABLES = "pipeline_activities + cfr_mttr_metric_data"

_PROJECT_URL   = "https://github.com/demo-acme/project_001.git"
_ISSUE_PROJECT = "Acme Platform"
_RECORD_BY     = "seed-data"
_PIPELINE_ID   = "a1b2c3d4-0000-0000-0000-demoaacme0001"  # stable across runs

_MONTHS = [
    (2025,  4), (2025,  5), (2025,  6),
    (2025,  7), (2025,  8), (2025,  9),
    (2025, 10), (2025, 11), (2025, 12),
    (2026,  1), (2026,  2), (2026,  3),
]
_N    = len(_MONTHS) - 1
_D0   = date(2025, 4, 1)
_D1   = date(2026, 3, 31)
_SPAN = (_D1 - _D0).days


def _mt(yr, mo):
    return _MONTHS.index((yr, mo)) / _N


def _business_days(yr, mo):
    d, out = date(yr, mo, 1), []
    while d.month == mo:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _spread(pool, n):
    """Pick n evenly-spaced items from pool without overflow."""
    if not pool:
        return []
    n = min(n, len(pool))
    if n == 0:
        return []
    step = len(pool) / n
    return [pool[int(i * step)] for i in range(n)]


# ── SQL helpers ────────────────────────────────────────────────────────────────

def _sq(s):
    return "'" + str(s).replace("'", "\\'") + "'"


def _ts(dt):
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


# ── pipeline_activities rows (DF chart) ───────────────────────────────────────

def _pa_rows(catalog):
    rows = []
    run  = 0
    for yr, mo in _MONTHS:
        t      = _mt(yr, mo)
        s      = yr * 100 + mo
        total  = jitter(round(lerp(8, 80, t)), 12, s)
        frate  = lerp(0.42, 0.04, t)
        if (yr, mo) == (2026, 3):
            frate = 0.22
        failed = max(0, round(total * frate))

        for i, d in enumerate(_spread(_business_days(yr, mo), total)):
            run += 1
            rng      = random.Random(s * 1000 + i)
            started  = datetime(yr, mo, d.day, rng.randint(9, 17), rng.randint(0, 59))
            finished = started + timedelta(minutes=rng.randint(5, 30))
            status   = "failed" if i < failed else "success"
            conc     = "Failed" if i < failed else "Succeeded"

            rows.append(
                f"  ({_sq('github')}, {_sq(_PROJECT_URL)}, {_sq('project_001')}, "
                f"{_sq(_PIPELINE_ID)}, {_sq(str(run))}, {_sq('acme-deploy-pipeline')}, "
                f"{_sq(status)}, {_ts(started)}, {_ts(finished)}, "
                f"{_sq(f'step-{run:05d}')}, {_sq('Deploy')}, {_sq(status)}, {_sq(conc)}, "
                f"{_ts(started)}, {_ts(finished)}, "
                f"{_sq('main')}, {_sq(_RECORD_BY)}, "
                f"CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"
            )
    return rows


# ── cfr_mttr_metric_data rows (CFR + MTTR charts) ─────────────────────────────

def _cfr_rows(catalog):
    rows  = []
    issue = 0
    for yr, mo in _MONTHS:
        t      = _mt(yr, mo)
        s      = yr * 100 + mo
        total  = jitter(round(lerp(8, 80, t)), 12, s)
        frate  = lerp(0.42, 0.04, t)
        if (yr, mo) == (2026, 3):
            frate = 0.22
        failed = max(0, round(total * frate))

        avg_mttr = round(max(0.3, lerp(200.0, 0.5, t) + random.Random(s + 3).gauss(0, 3)), 2)
        if (yr, mo) == (2026, 3):
            avg_mttr = 18.0
        tot_inc = jitter(round(lerp(8, 1, t)), 20, s + 4)

        bdays = _business_days(yr, mo)

        # Change + failure issues (one per deployment, drives CFR)
        for i, d in enumerate(_spread(bdays, total)):
            issue += 1
            rng     = random.Random(s * 1000 + i)
            created = datetime(yr, mo, d.day, rng.randint(9, 17), rng.randint(0, 59))
            # Resolution 2 hours after creation (not an incident, just a Jira ticket closed)
            resolved = created + timedelta(hours=2)
            key      = f"ACME-{issue:04d}"
            fail_key = _sq(key) if i < failed else "NULL"

            rows.append(
                f"  ({_sq(key)}, {_sq('Story')}, {_sq('Closed')}, {_sq(_ISSUE_PROJECT)}, "
                f"{_sq('Done')}, {_ts(created)}, {_ts(resolved)}, "
                f"{_sq('jira')}, {_ts(resolved)}, {_sq('Medium')}, "
                f"{_sq('Demo User')}, {_sq(f'https://acme.atlassian.net/browse/{key}')}, "
                f"{_sq(f'Deploy {yr}-{mo:02d} #{i+1}')}, "
                f"NULL, {fail_key}, {_sq(key)}, "  # mttr_key=NULL, fail_key, changes_key
                f"NULL, CURRENT_TIMESTAMP(), {_sq(_RECORD_BY)})"
            )

        # Incident issues (drives MTTR — mttr_issue_key IS NOT NULL)
        for j, d in enumerate(_spread(bdays, tot_inc)):
            issue += 1
            rng     = random.Random(s * 10000 + j)
            created = datetime(yr, mo, d.day, rng.randint(0, 23), rng.randint(0, 59))
            # Jitter resolution time around avg_mttr (±30%)
            mttr_h  = max(0.1, avg_mttr * (0.7 + rng.random() * 0.6))
            resolved = created + timedelta(hours=mttr_h)
            key      = f"ACME-INC-{issue:04d}"

            rows.append(
                f"  ({_sq(key)}, {_sq('Bug')}, {_sq('Closed')}, {_sq(_ISSUE_PROJECT)}, "
                f"{_sq('Fixed')}, {_ts(created)}, {_ts(resolved)}, "
                f"{_sq('jira')}, {_ts(resolved)}, {_sq('High')}, "
                f"{_sq('Demo User')}, {_sq(f'https://acme.atlassian.net/browse/{key}')}, "
                f"{_sq(f'Incident {yr}-{mo:02d} #{j+1}')}, "
                f"{_sq(key)}, NULL, NULL, "  # mttr_key=key, no CFR tracking
                f"NULL, CURRENT_TIMESTAMP(), {_sq(_RECORD_BY)})"
            )
    return rows


# ── Public API ─────────────────────────────────────────────────────────────────

_PA_HDR = """\
INSERT INTO {catalog}.base_datasets.pipeline_activities
  (pipeline_source, project_url, project_name, pipeline_id,
   pipeline_run_count, pipeline_name, pipeline_status,
   pipeline_started_at, pipeline_finished_at,
   step_id, step_type, step_status, step_conclusion,
   step_started_at, step_finished_at,
   branch, record_inserted_by,
   record_insert_date, source_record_insert_date)
VALUES
{values}"""

_CFR_HDR = """\
INSERT INTO {catalog}.base_datasets.cfr_mttr_metric_data
  (issue_key, issue_type, issue_status, issue_project,
   issue_resolution_name, issue_created, issue_updated,
   itsm_source, issue_resolution_date, issue_priority,
   assignee_name, issue_link, issue_summary,
   mttr_issue_key, cfr_total_failures_key, cfr_total_changes_key,
   fix_version, record_inserted_datetime, record_inserted_by)
VALUES
{values}"""


def generate(catalog, entities, story):
    stmts = []

    pa = _pa_rows(catalog)
    for i in range(0, len(pa), 400):
        stmts.append(_PA_HDR.format(catalog=catalog, values=",\n".join(pa[i:i+400])))

    cfr = _cfr_rows(catalog)
    for i in range(0, len(cfr), 400):
        stmts.append(_CFR_HDR.format(catalog=catalog, values=",\n".join(cfr[i:i+400])))

    return stmts
