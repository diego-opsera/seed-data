"""
transform_stage.mt_itsm_issues_hist + mt_itsm_issues_current — Jira issues.

Feeds Quality (defect leakage) and Impact (features shipped).

Writes BOTH tables because the views split:
    base_datasets.v_itsm_issues_hist     <- mt_itsm_issues_hist     (DVI reads this)
    base_datasets.v_itsm_issues_current  <- mt_itsm_issues_current
Both are plain passthroughs — `SELECT *, regexp_replace(assignee_email,'@.*','')
AS assignee_id FROM transform_stage.<t>` — with no org filter and no MAX trick,
so rows surface as soon as they are inserted.

Literal sets that decide whether a row counts (dviQueries.js):

  Quality counts a defect only when
      LOWER(issue_type) IN ('bug','defect')
    AND UPPER(issue_priority) IN ('BLOCKER','CRITICAL','HIGHEST','1','1 - CRITICAL')
  This is BUGS.md #13 — the shared generator emits high/medium/low, so
  demo-acme-direct's defects have never counted. We emit HIGHEST / BLOCKER.

  Impact counts a feature only when it is resolved:
      issue_resolution_date IS NOT NULL
    AND issue_updated_date inside the ETL window
    AND LOWER(issue_status) in the completion set
    AND LOWER(issue_type) IN ('story','feature','new feature','enhancement',
                              'epic','user story','product backlog item')
"""
from datetime import date, datetime, timedelta

from .sqlutil import (arc_t, beat_for, chunked_inserts, dt, json_lit, lerp,
                      seeded, sq, ts)

TABLES = ("transform_stage.mt_itsm_issues_hist", "transform_stage.mt_itsm_issues_current")
COLUMNS = (
    "itsm_source, data_source, issue_key, issue_link, issue_id, issue_type, "
    "issue_summary, issue_priority, issue_project, issue_project_key, "
    "issue_resolution_name, issue_created, issue_updated, issue_resolution_date, "
    "issue_status, assignee_name, assignee_email, customer_id, record_inserted_by, "
    "issue_created_date, issue_updated_date, story_points, team_name, "
    "source_record_insert_datetime, record_insert_datetime, source_record_insert_date"
)
INSERTED_BY = "seed-data-vf"

DONE_STATUSES = ["done", "closed", "resolved", "completed", "product deployment"]
OPEN_STATUSES = ["open", "in progress", "qa testing", "peer review", "to do"]
FEATURE_TYPES = ["story", "story", "story", "epic", "feature", "enhancement"]
STORY_POINTS = ["1", "2", "3", "5", "8", "13"]


def generate(catalog: str, entities: dict, story: dict, *,
             date_from: str | None = None, date_to: str | None = None) -> list[str]:
    """
    Volume is driven per MONTH from features_per_month, not from a per-developer
    rate. Impact is a raw COUNT against thresholds of 12/8/3, so a natural issue
    volume for 25 developers (~200 resolved features a month) pins the tile at
    100 and the arc becomes invisible. Working backwards from the target keeps
    the tile on its curve.

    Per month:
        resolved features = features_per_month(t) x beat.throughput   -> Impact
        open features     = 40% of that                              -> realism
        defects           = whatever makes defects/total = defect_share -> Quality
    """
    from .story import roster

    org = entities["orgs"][0]["name"]
    proj = entities["jira_project"]
    users = roster(entities, story)
    arc_start = date.fromisoformat(story["start_date"])
    arc_end = date.fromisoformat(story["end_date"])
    lo = date.fromisoformat(date_from) if date_from else arc_start
    hi = date.fromisoformat(date_to) if date_to else arc_end

    defect_lo = float(story["defect_share_start"])
    defect_hi = float(story["defect_share_end"])
    feat_lo = float(story["features_per_month_start"])
    feat_hi = float(story["features_per_month_end"])
    hi_labels = story["high_priority_labels"]
    lo_labels = story["low_priority_labels"]

    values = []
    seq = 0
    for month_start in _months_between(lo, hi):
        month_end = min(_month_last_day(month_start), hi)
        days = [d for d in _days(max(month_start, lo), month_end) if d.weekday() < 5]
        if not days:
            continue

        t = arc_t(month_start, arc_start, arc_end)
        beat = beat_for(month_start, arc_start, story)
        n_feat_done = max(1, round(lerp(feat_lo, feat_hi, t) * float(beat.get("throughput", 1.0))))
        n_feat_open = max(0, round(n_feat_done * 0.4))
        share = min(0.6, lerp(defect_lo, defect_hi, t) * float(beat.get("quality", 1.0)))
        n_feat_total = n_feat_done + n_feat_open
        n_defects = max(0, round(n_feat_total * share / max(1e-6, 1.0 - share)))

        plan = ([("feature", True)] * n_feat_done
                + [("feature", False)] * n_feat_open
                + [("defect", True)] * round(n_defects * 0.7)
                + [("defect", False)] * (n_defects - round(n_defects * 0.7)))

        for i, (kind, resolved) in enumerate(plan):
            seq += 1
            user = users[seq % len(users)]
            day = days[i % len(days)]
            rng = seeded(month_start, seq, "itsm")
            key = f"{proj['key']}-{seq}"

            if kind == "defect":
                issue_type = "bug"
                priority = hi_labels[seq % len(hi_labels)]      # counted by Quality
            else:
                issue_type = FEATURE_TYPES[seq % len(FEATURE_TYPES)]
                priority = lo_labels[seq % len(lo_labels)]      # deliberately not counted

            created_dt = datetime(day.year, day.month, day.day, rng.randint(9, 16))
            res_dt = None
            if resolved:
                cycle_days = max(1, round(rng.uniform(2, 16) * (1.3 if kind == "defect" else 1.0)))
                candidate = created_dt + timedelta(days=cycle_days, hours=rng.randint(0, 6))
                # Resolution has to land inside the window or Impact will not see it.
                if candidate.date() <= hi:
                    res_dt = candidate
                else:
                    res_dt = datetime(hi.year, hi.month, hi.day, 17)

            if res_dt is not None:
                status = DONE_STATUSES[seq % len(DONE_STATUSES)]
                updated_dt = res_dt
                resolution_name = "Fixed" if kind == "defect" else "Done"
            else:
                status = OPEN_STATUSES[seq % len(OPEN_STATUSES)]
                updated_dt = min(created_dt + timedelta(days=rng.randint(0, 6)),
                                 datetime(hi.year, hi.month, hi.day, 17))
                resolution_name = None

            pts = "2" if kind == "defect" else STORY_POINTS[seq % len(STORY_POINTS)]
            ins_dt = f"{updated_dt.date().isoformat()} 23:00:00"

            values.append(
                "  ("
                f"{sq('jira')}, {sq('jira api')}, {sq(key)}, "
                f"{sq(f'https://demo-acme-vf.atlassian.net/browse/{key}')}, "
                f"{sq(str(900000 + seq))}, {sq(issue_type)}, "
                f"{sq(_summary(rng, issue_type, user))}, {sq(priority)}, "
                f"{sq(proj['name'])}, {sq(proj['key'])}, {sq(resolution_name)}, "
                f"{ts(created_dt.strftime('%Y-%m-%d %H:%M:%S'))}, "
                f"{ts(updated_dt.strftime('%Y-%m-%d %H:%M:%S'))}, "
                f"{ts(res_dt.strftime('%Y-%m-%d %H:%M:%S') if res_dt else None)}, "
                f"{sq(status)}, {sq(user['name'])}, {sq(user['email'])}, "
                f"{sq(org)}, {sq(INSERTED_BY)}, "
                f"{dt(created_dt.date())}, {dt(updated_dt.date())}, "
                f"{sq(pts)}, {sq(user['team'])}, "
                f"{ts(ins_dt)}, {ts(ins_dt)}, {dt(updated_dt.date())}"
                ")"
            )

    out = []
    for table in TABLES:
        out.extend(chunked_inserts(f"{catalog}.{table}", COLUMNS, values, batch=400))
    return out


def _months_between(lo: date, hi: date) -> list[date]:
    out, cur = [], lo.replace(day=1)
    while cur <= hi:
        out.append(cur)
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    return out


def _month_last_day(month_start: date) -> date:
    nxt = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return nxt - timedelta(days=1)


def _days(lo: date, hi: date) -> list[date]:
    out, cur = [], lo
    while cur <= hi:
        out.append(cur)
        cur += timedelta(days=1)
    return out


_FEATURE_SUMMARIES = [
    "Surface retry status on the ingest dashboard",
    "Support partial refunds in the payments flow",
    "Add per-team filters to the delivery report",
    "Expose pipeline duration in the run detail view",
    "Let admins bulk-assign mapping groups",
]
_BUG_SUMMARIES = [
    "Webhook retries duplicate the payment record",
    "Sprint rollup drops the final day of the window",
    "Filter values leak across projects",
    "Run detail 500s when the job has no logs",
    "Timezone offset shifts the daily bucket",
]


def _summary(rng, issue_type: str, user: dict) -> str:
    pool = _BUG_SUMMARIES if issue_type in ("bug", "defect") else _FEATURE_SUMMARIES
    return rng.choice(pool)


def delete_sql(catalog: str, entities: dict) -> list[str]:
    org = entities["orgs"][0]["name"]
    return [
        f"DELETE FROM {catalog}.{t} "
        f"WHERE customer_id = '{org}' AND record_inserted_by = '{INSERTED_BY}';"
        for t in TABLES
    ]
