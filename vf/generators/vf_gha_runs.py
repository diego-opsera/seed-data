"""
source_to_stage.github_actions_runs_rest_api — GitHub Actions workflow runs.

Feeds two things:
  - Security, via the per-developer 'gha-per-repo' path. This matters because
    tier 1 (asp_sonar_issues) is whole-catalog and unscoped, so the ORG security
    number is pinned at ~4/100 by other tenants' open vulnerabilities and cannot
    be fixed by seeding (BUGS.md #14). Per-developer security instead uses each
    repo's GHA pass rate, weighted by the developer's activity in that repo — so
    high-passing runs on OUR repos give OUR cohort a real score.
  - Impact's monthly trend, via deploy-workflow counts, when
    raw_mongo_pipelineactivities has nothing for the org.

A run counts as a "security gate" when its $.name matches any of
  %security% %scan% %codeql% %sast% %dast% %vulnerability% %snyk% %trivy%
  %dependabot% %quality% %lint% %test%
(syncDvi ghaSecurityMetricsQuery). Workflow names come from the story's
security_workflow_names so the match is deliberate rather than accidental —
note how loosely prod matches: this catalog's top workflow is
"Update DeleteQATesting12" with 7,792 failures, counted purely because of
'%test%', which is most of why tier 3 reads 34%.

record_insert_datetime MUST be populated: ghaRunsQuery and
ghaSecurityMetricsQuery both filter on it, and many prod rows have it NULL and
are silently excluded.
"""
from datetime import date, timedelta

from .sqlutil import (arc_t, beat_for, chunked_inserts, iso_z, json_lit, lerp,
                      seeded, sha, sq, stable_seed, ts)

TABLE = "source_to_stage.github_actions_runs_rest_api"
COLUMNS = ("repo_url, message, owner, record_updated_timestamp, "
           "record_insert_datetime, record_inserted_by")
INSERTED_BY = "seed-data-vf"

# Named so the Impact deploy-count path can pick them out.
DEPLOY_WORKFLOWS = ["Build and Deploy", "Release to Production"]


def generate(catalog: str, entities: dict, story: dict, *,
             date_from: str | None = None, date_to: str | None = None) -> list[str]:
    from .story import roster

    org = entities["orgs"][0]["name"]
    repos = entities["repos"]
    users = roster(entities, story)
    by_team = {}
    for u in users:
        by_team.setdefault(u["team"], []).append(u)

    arc_start = date.fromisoformat(story["start_date"])
    arc_end = date.fromisoformat(story["end_date"])
    lo = date.fromisoformat(date_from) if date_from else arc_start
    hi = date.fromisoformat(date_to) if date_to else arc_end

    sec_names = story["security_workflow_names"]
    all_names = list(sec_names) + DEPLOY_WORKFLOWS
    pass_lo = float(story["gha_success_rate_start"])
    pass_hi = float(story["gha_success_rate_end"])
    runs_lo = float(story["gha_runs_per_repo_per_day_start"])
    runs_hi = float(story["gha_runs_per_repo_per_day_end"])

    values = []
    run_number = 500
    day = lo
    while day <= hi:
        if day.weekday() >= 5:
            day += timedelta(days=1)
            continue

        t = arc_t(day, arc_start, arc_end)
        beat = beat_for(day, arc_start, story)
        pass_rate = min(0.99, lerp(pass_lo, pass_hi, t) / float(beat.get("quality", 1.0)))
        per_repo = lerp(runs_lo, runs_hi, t) * float(beat.get("throughput", 1.0))

        for repo in repos:
            rng = seeded(day, repo["name"], "gha")
            n = max(0, round(per_repo + rng.uniform(-1.2, 1.2)))
            team_users = by_team.get(repo["team"]) or users

            for seq in range(n):
                run_number += 1
                wf = all_names[(run_number + seq) % len(all_names)]
                actor = team_users[(run_number + seq) % len(team_users)]
                hour = rng.randint(8, 19)
                ok = rng.random() < pass_rate
                conclusion = "success" if ok else rng.choice(["failure", "failure", "cancelled"])
                head_sha = sha(stable_seed(repo["name"], run_number))

                payload = {
                    "id": 30_000_000_000 + run_number,
                    "name": wf,
                    "node_id": f"WFR_{head_sha[:16]}",
                    "head_branch": "main",
                    "head_sha": head_sha,
                    "path": f".github/workflows/{wf.lower().replace(' ', '-')}.yml",
                    "display_title": wf,
                    "run_number": run_number,
                    "event": "push",
                    "status": "completed",
                    "conclusion": conclusion,
                    "workflow_id": 100000 + (run_number % 50),
                    "url": f"https://api.github.com/repos/{repo['name']}/actions/runs/{30_000_000_000 + run_number}",
                    "html_url": f"{repo['html_url']}/actions/runs/{30_000_000_000 + run_number}",
                    "created_at": iso_z(day, hour),
                    "updated_at": iso_z(day, min(hour + 1, 23)),
                    "run_started_at": iso_z(day, hour),
                    "actor": {"login": actor["login"], "id": actor["id"], "type": "User"},
                    "triggering_actor": {"login": actor["login"], "id": actor["id"], "type": "User"},
                    "repository": {
                        "name": repo["name"].split("/")[-1],
                        "full_name": repo["name"],
                        "owner": {"login": org, "type": "Organization"},
                        "html_url": repo["html_url"],
                    },
                }
                insert_dt = f"{day.isoformat()} {min(hour + 1, 23):02d}:45:00"
                values.append(
                    f"  ({sq(repo['html_url'] + '.git')}, {json_lit(payload)}, {sq(org)}, "
                    f"{ts(insert_dt)}, {ts(insert_dt)}, {sq(INSERTED_BY)})"
                )
        day += timedelta(days=1)

    return chunked_inserts(f"{catalog}.{TABLE}", COLUMNS, values, batch=300)


def delete_sql(catalog: str, entities: dict) -> list[str]:
    org = entities["orgs"][0]["name"]
    return [
        f"DELETE FROM {catalog}.{TABLE} "
        f"WHERE owner = '{org}' AND record_inserted_by = '{INSERTED_BY}';"
    ]
