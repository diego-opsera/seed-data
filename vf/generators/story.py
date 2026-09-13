"""
Story + roster loading for the VisualForge (demo-acme-vf) batch.

Wraps generators.utils rather than forking it — the shared helpers (lerp,
trend_base, day_scale, expand_users, US_HOLIDAYS, …) are reused unchanged. Two
things are VisualForge-specific and live here:

  1. FORWARD-DATING. generators.utils.load_story() always sets
     end_date = today. The DVI demo has to seed past today (see the WHY
     FORWARD-DATE note in vf/config/stories/dvi.yaml), so load_dvi_story()
     extends end_date by the story's `forward_days`.

  2. ROSTER IDENTITY. Every vf/ generator must emit the SAME email for a given
     developer, because VisualForge joins commits / PRs / Jira / Sonar / AI
     usage on LOWER(TRIM(email)). roster() is the single source of that mapping
     and refuses to return an unvalidated roster.
"""
import os
import sys
from datetime import date, timedelta

import yaml

# Shared helpers live in the repo root package; notebooks put /tmp/seed-data on
# sys.path, but support running from a checkout too.
for _base in (".", "/tmp/seed-data", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")):
    if os.path.isdir(os.path.join(_base, "generators")) and _base not in sys.path:
        sys.path.insert(0, os.path.abspath(_base))
        break

from generators.utils import expand_users, load_story  # noqa: E402

from .identity_gates import verify_roster  # noqa: E402

VF_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENTITIES_PATH = os.path.join(VF_DIR, "config", "entities.yaml")

ORG_NAME = "demo-acme-vf"


def load_dvi_story(name: str = "dvi", *, window_days: int = 365) -> dict:
    """
    Load a vf/ story with the rolling window EXTENDED forward.

    generators.utils.load_story() gives start_date = today - window_days and
    end_date = today. This pushes end_date out by `forward_days` so generators
    emit rows for months that have not happened yet — harmless, because every
    ETL query is bounded by TO_DATE = currentDate(), and it stops the demo from
    decaying when the month rolls over.

    Adds:
      forward_end_date — same as end_date, kept explicit for readability
      today            — ISO date the arc was built against
    """
    story = _load_story_from_vf(name, window_days=window_days)
    forward_days = int(story.get("forward_days", 0) or 0)
    today = date.today()
    if forward_days > 0:
        story["end_date"] = (today + timedelta(days=forward_days)).isoformat()
    story["today"] = today.isoformat()
    story["forward_end_date"] = story["end_date"]
    return story


def _load_story_from_vf(name: str, *, window_days: int) -> dict:
    """
    load_story() resolves config/stories/<name>.yaml relative to the repo root.
    vf/ keeps its own stories, so read ours directly and reuse load_story only
    for the date/event injection.
    """
    path = os.path.join(VF_DIR, "config", "stories", f"{name}.yaml")
    with open(path) as f:
        story = yaml.safe_load(f)
    # Borrow the shared date + event materialization, then merge our keys over it.
    scaffold = load_story("narrative", window_days=window_days)
    for key in ("start_date", "end_date", "events"):
        story[key] = scaffold[key]
    return story


def load_entities() -> dict:
    with open(ENTITIES_PATH) as f:
        return yaml.safe_load(f)


def roster(entities: dict | None = None, story: dict | None = None) -> list[dict]:
    """
    The demo-acme-vf developer roster, with language + ide assigned.

    Raises if any entry would be dropped by VisualForge's identity gates — a
    silent drop here costs a whole developer in individuals[] with no error
    surfaced anywhere downstream, so it is worth failing loudly at seed time.
    """
    entities = entities or load_entities()
    story = story or load_dvi_story()

    result = verify_roster(entities["users"])
    if not result["ok"]:
        lines = [f"  {who}: {p}" for who, ps in result["failures"].items() for p in ps]
        raise ValueError(
            "roster fails VisualForge identity gates — these developers would be "
            "silently dropped from individuals[]:\n" + "\n".join(lines)
        )

    users = expand_users(entities, story)
    for u in users:
        u["email"] = u["email"].strip().lower()
    return users


def email_by_login(entities: dict | None = None) -> dict:
    entities = entities or load_entities()
    return {u["login"].lower(): u["email"].strip().lower() for u in entities["users"]}


def repos_by_team(entities: dict | None = None) -> dict:
    """team name → list of repo dicts. Keeps a developer's commits, PRs and GHA
    runs on the repos their team actually owns."""
    entities = entities or load_entities()
    out = {}
    for repo in entities["repos"]:
        out.setdefault(repo["team"], []).append(repo)
    return out


def repo_for_user(user: dict, entities: dict | None = None, index: int = 0) -> dict:
    """Deterministic repo pick for a user — rotates within their team's repos."""
    entities = entities or load_entities()
    team_repos = repos_by_team(entities).get(user["team"]) or entities["repos"]
    return team_repos[index % len(team_repos)]


if __name__ == "__main__":
    st = load_dvi_story()
    ents = load_entities()
    devs = roster(ents, st)
    print("story      :", st["name"])
    print("arc        :", st["start_date"], "→", st["end_date"], f"(today {st['today']})")
    print("forward    :", st["forward_days"], "days past today")
    print("org        :", ORG_NAME)
    print("developers :", len(devs), "— all pass identity gates")
    print("teams      :", sorted({d["team"] for d in devs}))
    print("repos/team :", {k: [r["name"].split("/")[-1] for r in v] for k, v in repos_by_team(ents).items()})
    print("sample     :", {k: devs[0][k] for k in ("login", "email", "team", "language", "ide")})
