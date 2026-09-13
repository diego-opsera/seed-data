"""
source_to_stage.raw_github_pull_requests_rest_api_prs — merged PRs.

Feeds: Velocity (median cycle hours), Throughput (merged PR count), and the
monthly trend.

Table is 5 columns; the payload is a JSON string. Paths the ETL reads
(computeVelocityQueries.rawGithubPrsQuery):
    $.id  $.number  $.html_url  $.state  $.user.login
    $.created_at  $.merged_at  $.closed_at  $.title  $.merge_commit_sha
    $.base.repo.full_name  $.base.repo.owner.login
    $.additions  $.deletions  $.changed_files

Three constraints that decide whether a row counts at all:

  1. The ETL's WHERE is `merged_at IS NOT NULL` plus merged_at inside the window
     — this source yields MERGED PRs only. But syncDvi buckets by **created_at**
     (syncDvi.js:593), so a PR must be created AND merged inside the same month
     to land in that month's bucket.

  2. `TO_DATE(record_insert_datetime) <= <ETL end date>`, so record_insert_datetime
     is set just after merge — never far in the future, or forward-dated rows
     would be gated out on the day their month arrives.

  3. Cycle hours are kept only when `0 <= h < 10000`. The arc targets 96h → 26h;
     dviConfig thresholds are excellent 4 / good 8 / needsImprovement 24
     (inverted), so this lands in 'good' rather than pinning the tile at 100 the
     way the org-wide sub-1h medians do.

`additions` / `deletions` / `changed_files` are absent from real payloads here
(GitHub's list endpoint omits them), so prod rows carry no PR-level LOC. We
populate them.
"""
from datetime import date, timedelta

from generators.utils import active_user_count, day_scale

from .sqlutil import (arc_t, beat_for, chunked_inserts, iso_z, json_lit, lerp,
                      seeded, sha, sq, ts)

TABLE = "source_to_stage.raw_github_pull_requests_rest_api_prs"
COLUMNS = ("pull_requests, owner, record_insert_datetime, "
           "record_updated_timestamp, record_inserted_by")
INSERTED_BY = "seed-data-vf"


def generate(catalog: str, entities: dict, story: dict, *,
             date_from: str | None = None, date_to: str | None = None) -> list[str]:
    from .story import repo_for_user, roster

    org = entities["orgs"][0]["name"]
    users = roster(entities, story)
    arc_start = date.fromisoformat(story["start_date"])
    arc_end = date.fromisoformat(story["end_date"])
    lo = date.fromisoformat(date_from) if date_from else arc_start
    hi = date.fromisoformat(date_to) if date_to else arc_end

    cyc_start = float(story["pr_cycle_hours_start"])
    cyc_end = float(story["pr_cycle_hours_end"])
    wk_start = float(story["prs_per_dev_per_week_start"])
    wk_end = float(story["prs_per_dev_per_week_end"])

    values = []
    pr_number = 1000
    day = lo
    while day <= hi:
        if day.weekday() >= 5 or day_scale(day, story) == 0.0:
            day += timedelta(days=1)
            continue

        t = arc_t(day, arc_start, arc_end)
        beat = beat_for(day, arc_start, story)
        # Per-week rate spread over 5 working days.
        per_dev_day = (lerp(wk_start, wk_end, t) / 5.0) * float(beat.get("throughput", 1.0))
        # Velocity is inverted — the beat multiplier raises cycle time when
        # throughput drops (the migration months hurt both).
        cycle_target = lerp(cyc_start, cyc_end, t) * float(beat.get("velocity", 1.0))
        active = active_user_count(day, story, len(users))

        for user in users[:active]:
            rng = seeded(day, user["id"], "prs")
            if rng.random() > per_dev_day:
                continue

            pr_number += 1
            repo = repo_for_user(user, entities, index=user["id"])
            repo_full = repo["name"]

            open_hour = rng.randint(9, 15)
            # Per-developer spread around the arc target, floored so cycle time
            # never collapses to the sub-hour values that saturate the tile.
            hours = max(1.5, cycle_target * rng.uniform(0.55, 1.55))
            created = day
            merged_dt = _add_hours(created, open_hour, hours)
            # Merge must land inside the requested window or the row is invisible.
            if merged_dt.date() > hi:
                continue

            s = abs(hash((str(day), user["id"], pr_number))) % (2**31)
            merge_sha = sha(s)
            adds = rng.randint(round(lerp(40, 90, t)), round(lerp(220, 480, t)))
            dels = rng.randint(10, round(lerp(80, 200, t)))
            files = rng.randint(2, 14)

            payload = {
                "id": 3_000_000_000 + pr_number,
                "number": pr_number,
                "node_id": f"PR_{merge_sha[:18]}",
                "html_url": f"{repo['html_url']}/pull/{pr_number}",
                "url": f"https://api.github.com/repos/{repo_full}/pulls/{pr_number}",
                "state": "closed",
                "locked": False,
                "draft": False,
                "title": _title(rng, user),
                "user": {"login": user["login"], "id": user["id"], "type": "User"},
                "created_at": iso_z(created, open_hour),
                "updated_at": iso_z(merged_dt.date(), merged_dt.hour),
                "closed_at": iso_z(merged_dt.date(), merged_dt.hour),
                "merged_at": iso_z(merged_dt.date(), merged_dt.hour),
                "merge_commit_sha": merge_sha,
                "additions": adds,
                "deletions": dels,
                "changed_files": files,
                "head": {"ref": f"feature/{user['login'].split('.')[0]}-{pr_number}",
                         "sha": sha(s + 1)},
                "base": {
                    "ref": "main",
                    "repo": {
                        "id": 90000000 + (user["id"] % 1000),
                        "name": repo_full.split("/")[-1],
                        "full_name": repo_full,
                        "owner": {"login": org, "type": "Organization"},
                        "html_url": repo["html_url"],
                    },
                },
                "repo_id": str(90000000 + (user["id"] % 1000)),
                "repo_name": repo_full.split("/")[-1],
                "repo_url": repo["html_url"],
            }
            # Just after merge — satisfies the insert gate without being far future.
            insert_dt = f"{merged_dt.date().isoformat()} {min(merged_dt.hour + 1, 23):02d}:15:00"
            values.append(
                f"  ({json_lit(payload)}, {sq(org)}, {ts(insert_dt)}, "
                f"{ts(insert_dt)}, {sq(INSERTED_BY)})"
            )
        day += timedelta(days=1)

    return chunked_inserts(f"{catalog}.{TABLE}", COLUMNS, values, batch=200)


def _add_hours(day: date, start_hour: int, hours: float):
    from datetime import datetime
    return datetime(day.year, day.month, day.day, start_hour) + timedelta(hours=hours)


_TITLES = [
    "Reduce review turnaround on ingest changes",
    "Add idempotency key to the payment webhook",
    "Split the hierarchy resolver into its own module",
    "Backfill missing sprint metadata",
    "Cache the filter-values lookup",
    "Harden the retry path against partial writes",
    "Migrate the job runner off the legacy queue",
    "Add regression coverage for the rollup window",
]


def _title(rng, user) -> str:
    return f"[{user['team'].upper().replace('VF-', 'VF')}] {rng.choice(_TITLES)}"


def delete_sql(catalog: str, entities: dict) -> list[str]:
    org = entities["orgs"][0]["name"]
    return [
        f"DELETE FROM {catalog}.{TABLE} "
        f"WHERE owner = '{org}' AND record_inserted_by = '{INSERTED_BY}';"
    ]
