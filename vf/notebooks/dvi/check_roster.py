# Phase 1 — validate the demo-acme-vf roster and the DVI story arc.
#
# Checks every roster entry against VisualForge's identity gates before anything
# is seeded. A gate failure is SILENT downstream: the developer simply never
# appears in individuals[], which is the path that drives every DVI tile once it
# is non-empty. So this is the cheapest possible place to catch it.
#
# Touches no Databricks tables — pure config validation.
#
# Run:
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/check_roster.py").read())

import sys, os

# Databricks caches sys.modules across runs in a cluster session — flush ours so
# edits to vf/ and generators/ actually take effect (same pattern as
# notebooks/ai_compare/insert.py).
for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key.startswith("vf") or _key in ("loader",):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

from vf.generators.story import load_dvi_story, load_entities, roster, repos_by_team
from vf.generators.identity_gates import (
    check_user, email_to_name, email_to_initials, verify_roster,
)


def hr(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


entities = load_entities()
story = load_dvi_story()
users = entities["users"]

hr("1. Story arc")
print(f"  story        : {story['name']}")
print(f"  window       : {story['start_date']} → {story['end_date']}")
print(f"  today        : {story['today']}")
print(f"  forward_days : {story['forward_days']} (rows past today stay invisible until")
print(f"                 their month arrives — every ETL query is bounded by TO_DATE)")
print()
print("  Forward-dating matters because playground_prod is a one-time prod copy cut off")
print("  ~2026-08-02, and the DVI snapshot falls back only ONE month. Without rows past")
print("  today, Velocity and Throughput zero out on 2026-10-01 by themselves.")

hr("2. Org + scope")
print(f"  org_name     : {entities['orgs'][0]['name']} (id {entities['orgs'][0]['id']})")
print(f"  github_org   : {entities['github_org']}")
print(f"  jira project : {entities['jira_project']['key']} — {entities['jira_project']['name']}")
print(f"  teams        : {', '.join(t['name'] for t in entities['teams'])}")
print()
for team, repos in repos_by_team(entities).items():
    print(f"    {team:<14} {', '.join(r['name'] for r in repos)}")

hr("3. Identity gates — every developer must pass all five")
print("  isBot · isRealUserEmail · hasLikelyRealEmailLocalPart · isLikelyHumanLogin")
print("  · isLikelySyntheticDisplayName        (ported from sharedIdentity.js)")
print()
print(f"  {'':<6} {'login':<16} {'email':<40} {'UI label':<18} init")
print("  " + "-" * 74)
for u in users:
    problems = check_user(u)
    verdict = "PASS" if not problems else "FAIL"
    print(f"  {verdict:<6} {u['login']:<16} {u['email']:<40} "
          f"{email_to_name(u['email']):<18} {email_to_initials(u['email'])}")
    for p in problems:
        print(f"         └─ {p}")

result = verify_roster(users)

hr("4. Verdict")
if result["ok"]:
    print(f"  All {len(users)} developers pass. Emails below are the join key — every vf/")
    print("  generator must emit these verbatim in commit_email, PR author email,")
    print("  Jira assignee_email, Sonar author and AI tool usage. One mismatch drops that")
    print("  developer from individuals[] with no error anywhere.")
else:
    print("  FAILURES — fix vf/config/entities.yaml before seeding:")
    for who, problems in result["failures"].items():
        for p in problems:
            print(f"    {who}: {p}")

hr("5. Roster as the generators will see it")
devs = roster(entities, story)
print(f"  {len(devs)} developers, language + IDE assigned deterministically by user id")
print()
print(f"  {'email':<40} {'team':<14} {'language':<12} ide")
print("  " + "-" * 74)
for d in devs:
    print(f"  {d['email']:<40} {d['team']:<14} {d['language']:<12} {d['ide']}")

print()
print("Next: Phase 2 generators (vf/generators/) + vf/notebooks/dvi/insert.py")
