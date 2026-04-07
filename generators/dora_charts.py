"""
generators/dora_charts.py

Generates INSERT statements for the DORA individual metric chart tables:
  base_datasets.pipeline_activities        — DF chart (GitHub deployment events)
  base_datasets.cfr_mttr_metric_data       — CFR and MTTR charts (Jira issue data)
  base_datasets.pipeline_deployment_commits — CTFC chart (commit-to-deploy cycle time)

Arc mirrors generators/dora.py (Apr 2025 → Mar 2026) so chart values are
consistent with the SDM maturity badge scores in consumption_layer.sdm.

Scoped for safe deletion by:
  pipeline_activities:            record_inserted_by = 'seed-data'
  cfr_mttr_metric_data:           record_inserted_by = 'seed-data'
  pipeline_deployment_commits:    record_inserted_by = 'seed-data'

Requires filter_values_unity entries (inserted via insert_filter_group.py):
  filter_name='project_url',       filter_values=['https://github.com/demo-acme/project_001.git']
  filter_name='deployment_stages', filter_values=['deploy']
  filter_name='pipeline_status_success', filter_values=['success']
  filter_name='project_name',      filter_values=['Acme Platform']
  filter_name='project_url' (CTFC KPI), filter_values=['https://github.com/demo-acme/project_001.git']
"""

import hashlib
import random
from datetime import date, datetime, timedelta

from .utils import lerp, jitter

TABLES = "pipeline_activities + cfr_mttr_metric_data + pipeline_deployment_commits"

_PROJECT_URL   = "https://github.com/demo-acme/project_001.git"
_ISSUE_PROJECT = "Acme Platform"
_RECORD_BY     = "seed-data"
_PIPELINE_ID   = "a1b2c3d4-0000-0000-0000-demoaacme0001"  # stable across runs
_REPO_ID       = "demo-acme-001"
_OWNER         = "demo-acme"

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


def _make_sha(seed_str):
    return hashlib.sha1(seed_str.encode()).hexdigest()


# ── SQL helpers ────────────────────────────────────────────────────────────────

def _sq(s):
    return "'" + str(s).replace("'", "\\'") + "'"


def _ts(dt):
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def _td(dt):
    """Date-only timestamp (midnight) — used for *_date columns."""
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d')} 00:00:00'"


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
            # Successful deployments get a commit SHA so CTFC chart can join
            commit_sha = _sq(_make_sha(f'acme-deploy-{run}')) if status == "success" else "NULL"

            rows.append(
                f"  ({_sq('github')}, {_sq('github-workflow')}, {_sq(_PROJECT_URL)}, "
                f"{_sq('project_001')}, {_sq(_PIPELINE_ID)}, {_sq(str(run))}, {_sq('1')}, "
                f"{_sq('acme-deploy-pipeline')}, "
                f"{_sq(status)}, {_ts(started)}, {_ts(finished)}, "
                f"{_td(started)}, {_td(finished)}, "
                f"{_sq(f'step-{run:05d}')}, {_sq('deploy')}, {_sq('deploy')}, {_sq(status)}, {_sq(conc)}, "
                f"{_ts(started)}, {_ts(finished)}, "
                f"{_td(started)}, {_td(finished)}, "
                f"{_sq('main')}, {_sq('rest_api')}, {commit_sha}, {_sq(_RECORD_BY)}, "
                f"CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"
            )
    return rows


# ── pipeline_deployment_commits rows (CTFC chart) ─────────────────────────────

def _pdc_rows(catalog):
    rows     = []
    run      = 0
    prev_sha = _make_sha('acme-init')  # root "from" SHA

    for yr, mo in _MONTHS:
        t      = _mt(yr, mo)
        s      = yr * 100 + mo
        total  = jitter(round(lerp(8, 80, t)), 12, s)
        frate  = lerp(0.42, 0.04, t)
        if (yr, mo) == (2026, 3):
            frate = 0.22
        failed = max(0, round(total * frate))

        # CTFC target in days — mirrors dora.py lerp(22.0, 3.5, t)
        ctfc_days = max(0.5, lerp(22.0, 3.5, t))
        if (yr, mo) == (2026, 3):
            ctfc_days = 5.0  # slight regression during incident month

        for i, d in enumerate(_spread(_business_days(yr, mo), total)):
            run += 1
            is_success = (i >= failed)
            if not is_success:
                continue

            rng      = random.Random(s * 1000 + i)
            started  = datetime(yr, mo, d.day, rng.randint(9, 17), rng.randint(0, 59))
            to_sha   = _make_sha(f'acme-deploy-{run}')

            # 1-3 commits per deployment
            n_commits = rng.randint(1, 3)
            for j in range(n_commits):
                commit_sha    = _make_sha(f'acme-commit-{run}-{j}')
                jitter_factor = 0.7 + rng.random() * 0.6  # ±30% jitter
                ctfc_h        = max(1.0, ctfc_days * 24 * jitter_factor)
                committed_dt  = started - timedelta(hours=ctfc_h)
                web_url = f"https://github.com/demo-acme/project_001/commit/{commit_sha}"

                rows.append(
                    f"  ({_sq('github')}, {_sq(prev_sha)}, {_sq(to_sha)}, "
                    f"{_sq(_REPO_ID)}, {_ts(committed_dt)}, "
                    f"{_sq('dev@demo-acme.io')}, {_sq('Demo Developer')}, "
                    f"{_ts(committed_dt)}, {_sq(commit_sha)}, "
                    f"{_sq('feat: update deployment')}, {_sq('feat: update deployment')}, "
                    f"{_sq(web_url)}, "
                    f"array(), array('src/app.py'), array(), "
                    f"{_sq('Demo Developer')}, {_sq(commit_sha)}, "
                    f"{_sq(_OWNER)}, {_sq(_RECORD_BY)}, CURRENT_TIMESTAMP())"
                )

            prev_sha = to_sha
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
            resolved = created + timedelta(hours=2)
            key      = f"ACME-{issue:04d}"
            fail_key = _sq(key) if i < failed else "NULL"

            rows.append(
                f"  ({_sq(key)}, {_sq('Story')}, {_sq('Closed')}, {_sq(_ISSUE_PROJECT)}, "
                f"{_sq('Done')}, {_ts(created)}, {_ts(resolved)}, "
                f"{_sq('jira')}, {_ts(resolved)}, {_sq('Medium')}, "
                f"{_sq('Demo User')}, {_sq(f'https://acme.atlassian.net/browse/{key}')}, "
                f"{_sq(f'Deploy {yr}-{mo:02d} #{i+1}')}, "
                f"NULL, {fail_key}, {_sq(key)}, "
                f"NULL, CURRENT_TIMESTAMP(), {_sq(_RECORD_BY)})"
            )

        # Incident issues (drives MTTR — mttr_issue_key IS NOT NULL)
        for j, d in enumerate(_spread(bdays, tot_inc)):
            issue += 1
            rng     = random.Random(s * 10000 + j)
            created = datetime(yr, mo, d.day, rng.randint(0, 23), rng.randint(0, 59))
            mttr_h  = max(0.1, avg_mttr * (0.7 + rng.random() * 0.6))
            resolved = created + timedelta(hours=mttr_h)
            key      = f"ACME-INC-{issue:04d}"

            rows.append(
                f"  ({_sq(key)}, {_sq('Bug')}, {_sq('Closed')}, {_sq(_ISSUE_PROJECT)}, "
                f"{_sq('Fixed')}, {_ts(created)}, {_ts(resolved)}, "
                f"{_sq('jira')}, {_ts(resolved)}, {_sq('High')}, "
                f"{_sq('Demo User')}, {_sq(f'https://acme.atlassian.net/browse/{key}')}, "
                f"{_sq(f'Incident {yr}-{mo:02d} #{j+1}')}, "
                f"{_sq(key)}, NULL, NULL, "
                f"NULL, CURRENT_TIMESTAMP(), {_sq(_RECORD_BY)})"
            )
    return rows


# ── Public API ─────────────────────────────────────────────────────────────────

_PA_HDR = """\
INSERT INTO {catalog}.base_datasets.pipeline_activities
  (pipeline_source, tool_identifier, project_url, project_name,
   pipeline_id, pipeline_run_count, pipeline_run_attempt, pipeline_name,
   pipeline_status, pipeline_started_at, pipeline_finished_at,
   pipeline_started_date, pipeline_finished_date,
   step_id, step_type, step_name, step_status, step_conclusion,
   step_started_at, step_finished_at,
   step_started_date, step_finished_date,
   branch, data_source, pipeline_commit_sha, record_inserted_by,
   record_insert_date, source_record_insert_date)
VALUES
{values}"""

_PDC_HDR = """\
INSERT INTO {catalog}.base_datasets.pipeline_deployment_commits
  (pipeline_source, `from`, `to`, repository_id,
   committed_date, committer_email, committer_name, created_at,
   id, message, title, web_url,
   files_added, files_modified, files_removed,
   commit_author, commit_id, owner,
   record_inserted_by, record_inserted_datetime)
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

    pdc = _pdc_rows(catalog)
    for i in range(0, len(pdc), 400):
        stmts.append(_PDC_HDR.format(catalog=catalog, values=",\n".join(pdc[i:i+400])))

    cfr = _cfr_rows(catalog)
    for i in range(0, len(cfr), 400):
        stmts.append(_CFR_HDR.format(catalog=catalog, values=",\n".join(cfr[i:i+400])))

    return stmts
