"""
source_to_stage.raw_github_commits_rest_api — per-developer commits.

Feeds: per-developer Throughput, and (critically) the login→email map that lets
PRs resolve to a developer at all.

Table is 5 columns; the payload is a JSON string. Paths the DVI/Tokenomics ETL
reads (computeVelocityQueries.rawGithubRestApiCommitsQuery):
    $.sha  $.html_url  $.author.login
    $.commit.author.name  $.commit.author.email  $.commit.author.date
    $.stats.additions  $.stats.deletions  $.commit.message
    $.repository.full_name  $.repository.owner.login

Two deliberate differences from the prod rows in this catalog:

  1. `$.commit.author.email` is ALWAYS populated. This is the join key — the ETL
     requires an '@' before it will create a developer, and our
     base_datasets.commits_rest_api rows fail that today (BUGS.md #12: 37,598
     demo-acme-direct rows, 37,598 null emails).

  2. `$.repository` is populated. Prod payloads carry repo_id/repo_name/repo_url
     at the top level but no `repository` object, so `$.repository.full_name`
     resolves to null and per-repo attribution is dead on real rows. Ours works.
"""
from datetime import date, timedelta

from generators.utils import active_user_count, day_scale

from .sqlutil import (arc_t, beat_for, chunked_inserts, iso_z, json_lit, lerp,
                      seeded, sha, sq, stable_seed, ts)

TABLE = "source_to_stage.raw_github_commits_rest_api"
COLUMNS = "commit_details, org_name, owner, record_insert_datetime, record_inserted_by"
INSERTED_BY = "seed-data-vf"

_LANG_EXT = {"typescript": "ts", "python": "py", "go": "go", "kotlin": "kt", "csharp": "cs"}


def generate(catalog: str, entities: dict, story: dict, *,
             date_from: str | None = None, date_to: str | None = None) -> list[str]:
    from .story import repo_for_user, roster

    org = entities["orgs"][0]["name"]
    users = roster(entities, story)
    arc_start = date.fromisoformat(story["start_date"])
    arc_end = date.fromisoformat(story["end_date"])
    lo = date.fromisoformat(date_from) if date_from else arc_start
    hi = date.fromisoformat(date_to) if date_to else arc_end

    per_day_start = float(story["commits_per_dev_per_day_start"])
    per_day_end = float(story["commits_per_dev_per_day_end"])

    values = []
    day = lo
    while day <= hi:
        if day.weekday() >= 5 or day_scale(day, story) == 0.0:
            day += timedelta(days=1)
            continue

        t = arc_t(day, arc_start, arc_end)
        beat = beat_for(day, arc_start, story)
        per_dev = lerp(per_day_start, per_day_end, t) * float(beat.get("throughput", 1.0))
        active = active_user_count(day, story, len(users))

        for user in users[:active]:
            rng = seeded(day, user["id"], "commits")
            n = max(0, round(per_dev + rng.uniform(-0.8, 0.8)))
            repo = repo_for_user(user, entities, index=user["id"])
            ext = _LANG_EXT.get(user.get("language", "python"), "py")

            for seq in range(n):
                s = stable_seed(day, user["id"], seq, "c")
                commit_sha = sha(s)
                crng = seeded(day, user["id"], seq, "cd")
                adds = crng.randint(round(lerp(12, 40, t)), round(lerp(70, 180, t)))
                dels = crng.randint(0, round(lerp(20, 65, t)))
                hour = crng.randint(9, 18)
                repo_full = repo["name"]

                payload = {
                    "sha": commit_sha,
                    "node_id": f"C_{commit_sha[:20]}",
                    "html_url": f"{repo['html_url']}/commit/{commit_sha}",
                    "url": f"https://api.github.com/repos/{repo_full}/commits/{commit_sha}",
                    "commit": {
                        "author": {
                            "name": user["name"],
                            "email": user["email"],          # ← the join key
                            "date": iso_z(day, hour),
                        },
                        "committer": {
                            "name": user["name"],
                            "email": user["email"],
                            "date": iso_z(day, hour),
                        },
                        "message": f"{repo_full.split('/')[-1]}: {_subject(crng, ext)}",
                    },
                    "author": {"login": user["login"], "id": user["id"], "type": "User"},
                    "committer": {"login": user["login"], "id": user["id"], "type": "User"},
                    "parents": [{"sha": sha(s + 1)}],
                    "stats": {"additions": adds, "deletions": dels, "total": adds + dels},
                    "repository": {                          # ← absent in prod payloads
                        "id": 90000000 + (user["id"] % 1000),
                        "name": repo_full.split("/")[-1],
                        "full_name": repo_full,
                        "owner": {"login": org, "type": "Organization"},
                        "html_url": repo["html_url"],
                    },
                    "repo_id": str(90000000 + (user["id"] % 1000)),
                    "repo_name": repo_full.split("/")[-1],
                    "repo_url": repo["html_url"],
                    "branches": ["main"],
                }
                insert_dt = f"{day.isoformat()} {min(hour + 2, 23):02d}:30:00"
                values.append(
                    f"  ({json_lit(payload)}, {sq(org)}, {sq(org)}, "
                    f"{ts(insert_dt)}, {sq(INSERTED_BY)})"
                )
        day += timedelta(days=1)

    return chunked_inserts(f"{catalog}.{TABLE}", COLUMNS, values)


_SUBJECTS = [
    "handle retry budget on upstream timeouts",
    "extract pagination helper",
    "add contract test for webhook payload",
    "tighten input validation on the ingest path",
    "cache hierarchy lookups per request",
    "fix off-by-one in the window boundary",
    "drop the unused feature flag",
    "backfill missing index on lookup table",
]


def _subject(rng, ext: str) -> str:
    return rng.choice(_SUBJECTS) + f" (.{ext})"


def delete_sql(catalog: str, entities: dict) -> list[str]:
    org = entities["orgs"][0]["name"]
    return [
        f"DELETE FROM {catalog}.{TABLE} "
        f"WHERE org_name = '{org}' AND record_inserted_by = '{INSERTED_BY}';"
    ]
