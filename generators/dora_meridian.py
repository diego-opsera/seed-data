"""
generators/dora_meridian.py

Generates INSERT statements for Meridian Analytics DORA chart tables.

Story arc: manual Databricks workspace promotions (3-5 month LTFC) → Opsera (1-week LTFC).
Inflection at t=0.5 (midpoint of the date range — Oct 2025 in the default 12-month window).

Pre-Opsera (first half):
  Deployment Frequency:   lerp(2, 4, t_phase)    deploys/month
  Failure rate:           lerp(0.35, 0.30, t_phase)
  LTFC commit lag:        lerp(150, 90, t_phase)  days
  MTTR avg:               lerp(480, 240, t_phase) hours
  CTFC cycle time:        lerp(120, 60, t_phase)  days

Post-Opsera (second half):
  Deployment Frequency:   lerp(8, 60, t_phase)    deploys/month
  Failure rate:           lerp(0.30, 0.08, t_phase)
  LTFC commit lag:        lerp(90, 5, t_phase)    days
  MTTR avg:               lerp(240, 5, t_phase)   hours
  CTFC cycle time:        lerp(60, 5, t_phase)    days

Tables produced:
  base_datasets.pipeline_activities          — Deployment Frequency chart
  base_datasets.pipeline_deployment_commits  — Lead Time for Changes chart
  base_datasets.cfr_mttr_metric_data         — CFR + MTTR charts
  transform_stage.mt_itsm_issues_hist        — CTFC chart (data promotion tickets)
  transform_stage.mt_itsm_issues_current     — CTFC chart (mirrored)

Scoped for safe deletion by:
  pipeline_activities / cfr_mttr_metric_data / pipeline_deployment_commits:
      record_inserted_by = 'seed-data-meridian'
  mt_itsm_issues_hist / mt_itsm_issues_current:
      customer_id = 'demo-meridian'
"""

import hashlib
import random
from datetime import date, datetime, timedelta

from .utils import lerp, jitter

TABLES = (
    "pipeline_activities + pipeline_deployment_commits + "
    "cfr_mttr_metric_data + mt_itsm_issues_hist/current"
)

_PROJECT_URL   = "https://github.com/demo-meridian/data-platform.git"
_ISSUE_PROJECT = "Meridian Data Platform"   # CFR/MTTR filter: filter_name='project_name'
_JIRA_PROJECT  = "MDP"                       # CTFC: issue_project + filter_name='project_name'
_RECORD_BY     = "seed-data-meridian"
_PIPELINE_ID   = "b2c3d4e5-0000-0000-0000-meridian0001"
_REPO_ID       = "demo-meridian-001"
_OWNER         = "demo-meridian"
_CUSTOMER_ID   = "demo-meridian"
_BOARD_ID      = 2


# ── Month + phase helpers ──────────────────────────────────────────────────────

def _build_months(start: date, end: date):
    months = []
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def _phase_t(i, inflection_idx, total):
    """Piecewise t within each phase: 0→1 in pre, 0→1 in post."""
    if i < inflection_idx:
        return i / max(inflection_idx - 1, 1)
    else:
        return (i - inflection_idx) / max(total - inflection_idx - 1, 1)


# ── SQL helpers ────────────────────────────────────────────────────────────────

def _sq(s):
    return "'" + str(s).replace("'", "\\'") + "'"


def _ts(dt):
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def _td(dt):
    """Date-only timestamp (midnight) — used for *_date columns."""
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d')} 00:00:00'"


def _make_sha(seed_str):
    return hashlib.sha1(seed_str.encode()).hexdigest()


def _business_days(yr, mo):
    d, out = date(yr, mo, 1), []
    while d.month == mo:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _deploy_plan(months, inflection_idx):
    """
    Return list of (yr, mo, idx, n_deploys, failed_count, deploy_days) per month.
    All row generators use the same plan so data is internally consistent.

    Pre-Opsera: quarterly big-bang batches.
      Every 3rd month (idx % 3 == 2) is a maintenance window: 8-15 deployments
      concentrated in the last 10 business days of the month.
      Other months: 0-1 random emergency deploys (80% chance of 0).
    Post-Opsera: continuous flow ramping to near-daily.
    """
    total = len(months)
    plan  = []
    for idx, (yr, mo) in enumerate(months):
        t_phase = _phase_t(idx, inflection_idx, total)
        s       = yr * 100 + mo
        bdays   = _business_days(yr, mo)
        rng_d   = random.Random(s + 99)

        if idx < inflection_idx:
            is_maint = (idx % 3 == 2)
            if is_maint:
                n_deploys = max(8, jitter(12, 20, s))
                window    = bdays[-10:] if len(bdays) >= 10 else bdays
            else:
                n_deploys = 1 if random.Random(s + 71).random() < 0.20 else 0
                window    = bdays
            frate = lerp(0.35, 0.25, t_phase)
        else:
            n_deploys = max(1, jitter(round(lerp(8, 60, t_phase)), 15, s))
            frate     = lerp(0.30, 0.08, t_phase)
            window    = bdays

        failed      = max(0, round(n_deploys * frate))
        deploy_days = sorted(rng_d.choices(window, k=n_deploys)) if n_deploys > 0 else []
        plan.append((yr, mo, idx, n_deploys, failed, deploy_days))
    return plan


# ── pipeline_activities rows (Deployment Frequency chart) ─────────────────────

def _pa_rows(plan):
    rows = []
    run  = 0
    for yr, mo, idx, n_deploys, failed, deploy_days in plan:
        s = yr * 100 + mo
        for i, d in enumerate(deploy_days):
            run += 1
            rng      = random.Random(s * 1000 + i)
            started  = datetime(yr, mo, d.day, rng.randint(9, 17), rng.randint(0, 59))
            finished = started + timedelta(minutes=rng.randint(10, 60))
            status   = "failed" if i < failed else "success"
            conc     = "Failed"    if i < failed else "Succeeded"
            commit_sha = _sq(_make_sha(f'meridian-deploy-{run}')) if status == "success" else "NULL"

            rows.append(
                f"  ({_sq('github')}, {_sq('github-workflow')}, {_sq(_PROJECT_URL)}, "
                f"{_sq('data-platform')}, {_sq(_PIPELINE_ID)}, {_sq(str(run))}, {_sq('1')}, "
                f"{_sq('meridian-deploy-pipeline')}, "
                f"{_sq(status)}, {_ts(started)}, {_ts(finished)}, "
                f"{_td(started)}, {_td(finished)}, "
                f"{_sq(f'step-{run:05d}')}, {_sq('deploy')}, {_sq('deploy')}, "
                f"{_sq(status)}, {_sq(conc)}, "
                f"{_ts(started)}, {_ts(finished)}, "
                f"{_td(started)}, {_td(finished)}, "
                f"{_sq('main')}, {_sq('rest_api')}, {commit_sha}, {_sq(_RECORD_BY)}, "
                f"CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"
            )
    return rows


# ── pipeline_deployment_commits rows (Lead Time for Changes chart) ─────────────

def _pdc_rows(plan, inflection_idx):
    rows     = []
    run      = 0
    prev_sha = _make_sha('meridian-init')
    total    = len(plan)

    for yr, mo, idx, n_deploys, failed, deploy_days in plan:
        s       = yr * 100 + mo
        t_phase = _phase_t(idx, inflection_idx, total)

        if idx < inflection_idx:
            ctfc_days = lerp(150, 90, t_phase)
        else:
            ctfc_days = lerp(90, 5, t_phase)
        ctfc_days = max(0.5, ctfc_days)

        for i, d in enumerate(deploy_days):
            run += 1
            is_success = (i >= failed)
            if not is_success:
                continue

            rng     = random.Random(s * 1000 + i)
            started = datetime(yr, mo, d.day, rng.randint(9, 17), rng.randint(0, 59))
            to_sha  = _make_sha(f'meridian-deploy-{run}')

            # 1-2 commits per deployment (data platform changes are large batches)
            n_commits = rng.randint(1, 2)
            for j in range(n_commits):
                commit_sha    = _make_sha(f'meridian-commit-{run}-{j}')
                jitter_factor = 0.7 + rng.random() * 0.6
                ctfc_h        = max(1.0, ctfc_days * 24 * jitter_factor)
                committed_dt  = started - timedelta(hours=ctfc_h)
                web_url = (
                    f"https://github.com/demo-meridian/data-platform/commit/{commit_sha}"
                )

                rows.append(
                    f"  ({_sq('github')}, {_sq(prev_sha)}, {_sq(to_sha)}, "
                    f"{_sq(_REPO_ID)}, {_ts(committed_dt)}, "
                    f"{_sq('data-eng@demo-meridian.io')}, {_sq('Meridian Data Engineer')}, "
                    f"{_ts(committed_dt)}, {_sq(commit_sha)}, "
                    f"{_sq('feat: promote data pipeline to prod')}, "
                    f"{_sq('feat: promote data pipeline to prod')}, "
                    f"{_sq(web_url)}, "
                    f"array(), array('pipelines/transform.py'), array(), "
                    f"{_sq('Meridian Data Engineer')}, {_sq(commit_sha)}, "
                    f"{_sq(_OWNER)}, {_sq(_RECORD_BY)}, CURRENT_TIMESTAMP())"
                )

            prev_sha = to_sha
    return rows


# ── cfr_mttr_metric_data rows (CFR + MTTR charts) ─────────────────────────────

def _cfr_rows(plan, inflection_idx):
    rows  = []
    issue = 0
    total = len(plan)

    for yr, mo, idx, n_deploys, failed, deploy_days in plan:
        s       = yr * 100 + mo
        t_phase = _phase_t(idx, inflection_idx, total)
        # Use global t for incident count (simpler, monotone decline)
        global_t = idx / max(total - 1, 1)

        if idx < inflection_idx:
            avg_mttr = lerp(480, 240, t_phase)   # hours
        else:
            avg_mttr = lerp(240, 5, t_phase)     # hours
        avg_mttr = max(0.3, avg_mttr)

        tot_inc = max(0, jitter(round(lerp(5, 1, global_t)), 20, s + 4))
        bdays   = _business_days(yr, mo)

        # Change + failure issues — one per deployment (drives CFR)
        for i, d in enumerate(deploy_days):
            issue += 1
            rng      = random.Random(s * 1000 + i)
            created  = datetime(yr, mo, d.day, rng.randint(9, 17), rng.randint(0, 59))
            resolved = created + timedelta(hours=2)
            key      = f"MDP-{issue:04d}"
            fail_key = _sq(key) if i < failed else "NULL"

            rows.append(
                f"  ({_sq(key)}, {_sq('Story')}, {_sq('Closed')}, {_sq(_ISSUE_PROJECT)}, "
                f"{_sq('Done')}, {_ts(created)}, {_ts(resolved)}, "
                f"{_sq('jira')}, {_ts(resolved)}, {_sq('Medium')}, "
                f"{_sq('Meridian User')}, "
                f"{_sq(f'https://meridian.atlassian.net/browse/{key}')}, "
                f"{_sq(f'Deploy {yr}-{mo:02d} #{i+1}')}, "
                f"NULL, {fail_key}, {_sq(key)}, "
                f"NULL, CURRENT_TIMESTAMP(), {_sq(_RECORD_BY)})"
            )

        # Incident issues — drives MTTR (mttr_issue_key IS NOT NULL)
        rng_inc  = random.Random(s + 88)
        inc_days = sorted(rng_inc.choices(bdays, k=tot_inc)) if tot_inc > 0 else []
        for j, d in enumerate(inc_days):
            issue  += 1
            rng     = random.Random(s * 10000 + j)
            created = datetime(yr, mo, d.day, rng.randint(0, 23), rng.randint(0, 59))
            mttr_h  = max(0.1, avg_mttr * (0.7 + rng.random() * 0.6))
            resolved = created + timedelta(hours=mttr_h)
            key      = f"MDP-INC-{issue:04d}"

            rows.append(
                f"  ({_sq(key)}, {_sq('Bug')}, {_sq('Closed')}, {_sq(_ISSUE_PROJECT)}, "
                f"{_sq('Fixed')}, {_ts(created)}, {_ts(resolved)}, "
                f"{_sq('jira')}, {_ts(resolved)}, {_sq('High')}, "
                f"{_sq('Meridian User')}, "
                f"{_sq(f'https://meridian.atlassian.net/browse/{key}')}, "
                f"{_sq(f'Incident {yr}-{mo:02d} #{j+1}')}, "
                f"{_sq(key)}, NULL, NULL, "
                f"NULL, CURRENT_TIMESTAMP(), {_sq(_RECORD_BY)})"
            )

    return rows


# ── mt_itsm_issues rows (CTFC chart — data promotion Jira tickets) ─────────────

def _itsm_rows(plan, inflection_idx):
    """
    One promotion ticket per deploy-day.  issue_project='MDP' matches the CTFC
    filter_values_unity entry (filter_name='project_name', filter_values=['MDP']).
    board_info.board_id=2 matches filter_name='board_ids', filter_values=['2'].
    issue_updated = deploy date (resolution) — used as CTFC time axis.
    issue_created = issue_updated - cycle_days (may fall before story start; that's OK).
    """
    rows  = []
    issue = 0
    total = len(plan)

    board_info = (
        f"ARRAY(NAMED_STRUCT('board_id', CAST({_BOARD_ID} AS BIGINT), "
        f"'board_name', 'MDP Board', 'board_type', 'scrum'))"
    )

    for yr, mo, idx, n_deploys, failed, deploy_days in plan:
        s       = yr * 100 + mo
        t_phase = _phase_t(idx, inflection_idx, total)

        if idx < inflection_idx:
            cycle_days = lerp(120, 60, t_phase)
            priority   = "High"
        else:
            cycle_days = lerp(60, 5, t_phase)
            priority   = "Medium"
        cycle_days = max(1, cycle_days)

        for i, d in enumerate(deploy_days):
            issue += 1
            rng = random.Random(s * 2000 + i)

            # Resolution date = deploy event
            resolved_dt = datetime(yr, mo, d.day, rng.randint(14, 18), rng.randint(0, 59))

            # Open date = resolved - cycle time (with ±30% jitter)
            jf          = 0.7 + rng.random() * 0.6
            cycle_h     = max(24.0, cycle_days * 24 * jf)
            created_dt  = resolved_dt - timedelta(hours=cycle_h)

            issue_key  = f"MDP-PROM-{issue:04d}"
            issue_id   = str(9900000 + issue)
            summary    = f"Promote data pipeline batch #{issue:04d} to production"
            issue_link = f"https://meridian.atlassian.net/browse/{issue_key}"
            created_d  = created_dt.strftime('%Y-%m-%d')
            resolved_d = resolved_dt.strftime('%Y-%m-%d')

            rows.append(
                f"  ('jira', 'cloud', 'rest_api_pull', "
                f"{_sq(issue_key)}, {_sq(issue_link)}, {_sq(issue_id)}, "
                f"'Task', "
                f"NULL, NULL, NULL, "                           # parent_issue_key/id/type
                f"{_sq(summary)}, NULL, "                       # summary, description
                f"{_sq(priority)}, NULL, "                      # priority, severity
                f"{_sq(_JIRA_PROJECT)}, {_sq(_JIRA_PROJECT)}, " # issue_project, issue_project_key
                f"'Done', "                                     # issue_resolution_name
                f"{_ts(created_dt)}, {_ts(resolved_dt)}, {_ts(resolved_dt)}, "  # created, updated, resolution_date
                f"'Done', 'status', "                           # issue_status, changelog_itemsfield
                f"{_ts(resolved_dt)}, "                         # timestamp
                f"'meridian-data-eng', 'data-eng@demo-meridian.io', "  # assignee
                f"{_sq(_CUSTOMER_ID)}, "                        # customer_id
                f"{_sq(_RECORD_BY)}, "                          # record_inserted_by
                f"DATE '{created_d}', DATE '{resolved_d}', "    # issue_created_date, issue_updated_date
                f"ARRAY(), "                                    # fix_version
                f"NULL, NULL, NULL, "                           # sprint_id, sprint_name, sprint_state
                f"NULL, NULL, NULL, NULL, "                     # sprint date cols
                f"NULL, "                                       # sprint_goal
                f"NULL, "                                       # story_points
                f"ARRAY(), "                                    # linked_issues
                f"NULL, NULL, "                                 # team_name, service_component
                f"NULL, NULL, "                                 # incident_start/end
                f"NULL, "                                       # opsera_team_name
                f"{board_info}, "
                f"ARRAY(), ARRAY(), ARRAY(), ARRAY(), NULL, "   # filter_info, labels, components, investment_category, service
                f"CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_DATE())"
            )

    return rows


# ── INSERT headers ─────────────────────────────────────────────────────────────

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

_ITSM_HDR = """\
INSERT INTO {{catalog}}.transform_stage.{table}
  (itsm_source, instance_type, data_source,
   issue_key, issue_link, issue_id, issue_type,
   parent_issue_key, parent_issue_id, parent_issue_type,
   issue_summary, issue_description, issue_priority, issue_severity,
   issue_project, issue_project_key,
   issue_resolution_name, issue_created, issue_updated, issue_resolution_date,
   issue_status, issue_changelog_itemsfield, timestamp,
   assignee_name, assignee_email, customer_id,
   record_inserted_by,
   issue_created_date, issue_updated_date,
   fix_version, sprint_id, sprint_name, sprint_state,
   sprint_start_date, sprint_end_date, sprint_complete_date, sprint_activated_date,
   sprint_goal, story_points,
   linked_issues, team_name, service_component,
   incident_start_time, incident_end_time, opsera_team_name,
   board_info, filter_info, labels, components, investment_category, service,
   source_record_insert_datetime, record_insert_datetime, source_record_insert_date)
VALUES
{{values}}"""

_ITSM_HIST_HDR    = _ITSM_HDR.format(table="mt_itsm_issues_hist")
_ITSM_CURRENT_HDR = _ITSM_HDR.format(table="mt_itsm_issues_current")


# ── Public API ─────────────────────────────────────────────────────────────────

def generate(catalog, entities, story):
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    months         = _build_months(start, end)
    inflection_idx = len(months) // 2
    plan           = _deploy_plan(months, inflection_idx)

    stmts = []

    # pipeline_activities
    pa = _pa_rows(plan)
    for i in range(0, len(pa), 400):
        stmts.append(_PA_HDR.format(catalog=catalog, values=",\n".join(pa[i:i+400])))

    # pipeline_deployment_commits
    pdc = _pdc_rows(plan, inflection_idx)
    for i in range(0, len(pdc), 400):
        stmts.append(_PDC_HDR.format(catalog=catalog, values=",\n".join(pdc[i:i+400])))

    # cfr_mttr_metric_data
    cfr = _cfr_rows(plan, inflection_idx)
    for i in range(0, len(cfr), 400):
        stmts.append(_CFR_HDR.format(catalog=catalog, values=",\n".join(cfr[i:i+400])))

    # mt_itsm_issues (hist + current) — CTFC chart
    itsm = _itsm_rows(plan, inflection_idx)
    for i in range(0, len(itsm), 400):
        batch = ",\n".join(itsm[i:i+400])
        stmts.append(_ITSM_HIST_HDR.format(catalog=catalog, values=batch))
        stmts.append(_ITSM_CURRENT_HDR.format(catalog=catalog, values=batch))

    return stmts
