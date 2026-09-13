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

# In the ETL completion set — these are what Impact counts.
DONE_STATUSES = ["done", "closed", "resolved", "completed", "product deployment"]
# Deliberately NOT in the completion set, but real values from this catalog. Used
# for work that is finished-ish yet uncounted, so issue volume can be realistic
# without every feature landing in Impact.
WIP_STATUSES = ["uat deployed", "master merge", "qa testing", "submit to qa", "peer review"]
OPEN_STATUSES = ["open", "in progress", "to do", "backlog"]
FEATURE_TYPES = ["story", "story", "story", "epic", "feature", "enhancement"]
STORY_POINTS = ["1", "2", "3", "5", "8", "13"]


def generate(catalog: str, entities: dict, story: dict, *,
             date_from: str | None = None, date_to: str | None = None) -> list[str]:
    """
    Volume comes from issues_per_dev_per_week, so all 25 developers get issues
    and the defect share is statistically meaningful.

    An earlier version drove total volume backwards from features_per_month to
    keep the Impact tile on its threshold curve (excellent = 12 resolved
    features). That produced 20 issues a month for 25 developers: five people had
    no Jira issue at all, and only two defects existed, so per-developer Quality
    and Impact were noise. Impact simply cannot track its curve for a realistic
    team — 25 developers completing two features each is 50/month against a
    threshold of 12 — so it saturates at 100 exactly like Throughput, and the
    per-developer path (normalized against the org P90) carries the spread.

    Per month:
        total issues = issues_per_dev_per_week(t) x devs x weeks x beat.throughput
        defects      = defect_share(t) x beat.quality x total    -> Quality
                       always HIGHEST/BLOCKER so they are actually counted
        features     = the rest; ~70% reach a completion status   -> Impact
                       the other 30% sit in WIP_STATUSES, resolution_date NULL
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
    wk_lo = float(story["issues_per_dev_per_week_start"])
    wk_hi = float(story["issues_per_dev_per_week_end"])
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
        weeks = max(1.0, len(days) / 5.0)
        total = max(len(users),
                    round(lerp(wk_lo, wk_hi, t) * len(users) * weeks
                          * float(beat.get("throughput", 1.0))))
        share = min(0.6, lerp(defect_lo, defect_hi, t) * float(beat.get("quality", 1.0)))
        n_defects = max(1, round(total * share))
        n_features = max(1, total - n_defects)
        n_feat_done = round(n_features * 0.70)
        n_feat_wip = n_features - n_feat_done
        n_def_done = round(n_defects * 0.70)

        # ('feature'|'defect', 'done'|'wip'|'open')
        plan = ([("feature", "done")] * n_feat_done
                + [("feature", "wip")] * n_feat_wip
                + [("defect", "done")] * n_def_done
                + [("defect", "open")] * (n_defects - n_def_done))

        for i, (kind, state) in enumerate(plan):
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
            if state == "done":
                cycle_days = max(1, round(rng.uniform(2, 16) * (1.3 if kind == "defect" else 1.0)))
                candidate = created_dt + timedelta(days=cycle_days, hours=rng.randint(0, 6))
                # Resolution must land inside the window or Impact never sees it.
                res_dt = candidate if candidate.date() <= hi else datetime(hi.year, hi.month, hi.day, 17)
                status = DONE_STATUSES[seq % len(DONE_STATUSES)]
                updated_dt = res_dt
                resolution_name = "Fixed" if kind == "defect" else "Done"
            else:
                # WIP and open rows keep resolution_date NULL, so they add to
                # Quality's denominator without counting toward Impact.
                pool = WIP_STATUSES if state == "wip" else OPEN_STATUSES
                status = pool[seq % len(pool)]
                updated_dt = min(created_dt + timedelta(days=rng.randint(0, 9)),
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
