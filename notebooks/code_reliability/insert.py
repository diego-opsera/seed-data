# Code Reliability dashboard — unified insert (Acme + Meridian in one place)
#
# This is the single home for every base table behind the
# /insights/v2/code-reliability/dashboard. Both demo orgs are seeded by the
# same generators; entities + story configs are picked up from the same
# YAML files used by the rest of the demo pipeline.
#
# Pattern mirrors notebooks/value_stream/insert.py — loop over both org
# configs, call generate(), execute the SQL.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/insert.py").read())

import sys, os, yaml

# Module cache-bust — ensures fresh generator code on re-run
for _key in list(sys.modules.keys()):
    if _key.startswith("generators"):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

from generators import dependabot_scan_alert, asp_sonar_issues

CATALOG = "playground_prod"

# ── Story configs ──────────────────────────────────────────────────────────────
# Both stories share the same date window (rewritten daily by master insert.py).

acme_story     = yaml.safe_load(open("config/stories/narrative.yaml"))
meridian_story = yaml.safe_load(open("config/stories/meridian_narrative.yaml"))
meridian_story["start_date"] = acme_story["start_date"]
meridian_story["end_date"]   = acme_story["end_date"]

# ── Entity configs ─────────────────────────────────────────────────────────────
# Acme entities come from config/entities.yaml; we swap in orgs[1] (the direct
# org demo-acme-direct) the same way notebooks/direct/insert.py does.
# Meridian entities are defined inline — kept in sync with notebooks/meridian/insert.py.

_acme_full = yaml.safe_load(open("config/entities.yaml"))
entities_acme = {**_acme_full, "orgs": [_acme_full["orgs"][1]]}

_meridian_org = {"id": 9990003, "name": "demo-meridian"}
entities_meridian = {
    "enterprise": {"id": 999997, "name": "demo-meridian"},
    "orgs": [_meridian_org, _meridian_org],
    "teams": [
        {"name": "data-engineering", "slug": "data-engineering", "org_name": "demo-meridian"},
    ],
    "users": [
        {"id": 9990101, "login": "meridian-alice", "assignee_login": "meridian-alice", "team": "data-engineering"},
        {"id": 9990102, "login": "meridian-bob",   "assignee_login": "meridian-bob",   "team": "data-engineering"},
        {"id": 9990103, "login": "meridian-carol", "assignee_login": "meridian-carol", "team": "data-engineering"},
    ],
    "languages": ["python"],
    "repos": [
        {"name": "demo-meridian/data-platform", "html_url": "https://github.com/demo-meridian/data-platform"},
    ],
    "ides":     ["vscode", "intellij", "visualstudio", "eclipse", "xcode"],
    "features": ["code_completion", "chat_panel_ask_mode", "chat_panel_edit_mode", "agent_edit"],
    "models":   ["gpt-4o", "gpt-4o-mini", "claude-3.7-sonnet", "claude-4.0-sonnet", "claude-4.5-sonnet", "o3-mini"],
}

ORG_CONFIGS = [
    ("Acme",     entities_acme,     acme_story),
    ("Meridian", entities_meridian, meridian_story),
]

# ── Generators that contribute to this dashboard ───────────────────────────────
# Add new generators (sonar, twistlock, was) below as we build them.

GENERATORS = [
    ("dependabot_scan_alert", dependabot_scan_alert),
    ("asp_sonar_issues",      asp_sonar_issues),
]

# ── Execute ────────────────────────────────────────────────────────────────────

for label, entities, story in ORG_CONFIGS:
    print(f"\n{'─'*60}")
    print(f"  {label}")
    print(f"{'─'*60}")
    for gen_label, gen_module in GENERATORS:
        statements = gen_module.generate(CATALOG, entities, story)
        if not statements:
            print(f"  [{gen_label}] no statements")
            continue
        for i, sql in enumerate(statements, 1):
            print(f"  [{gen_label} {i}/{len(statements)}] inserting...", end=" ")
            spark.sql(sql)
            print("done")

# ── Verify ─────────────────────────────────────────────────────────────────────
print(f"\n{'─'*60}\n  VERIFY: dependabot_scan_alert\n{'─'*60}")
spark.sql(f"""
    SELECT organization, severity, state, COUNT(*) AS n
    FROM {CATALOG}.base_datasets.dependabot_scan_alert
    WHERE organization IN ('demo-acme-direct', 'demo-meridian')
    GROUP BY organization, severity, state
    ORDER BY organization, severity, state
""").show(50, truncate=False)

print(f"\n{'─'*60}\n  VERIFY: asp_sonar_issues (latest scan per project per org)\n{'─'*60}")
spark.sql(f"""
    WITH latest AS (
        SELECT org_name, project, type, severity, status,
               source_record_insert_datetime,
               row_number() OVER (PARTITION BY org_name, project, branch
                                  ORDER BY source_record_insert_datetime DESC) AS rk
        FROM {CATALOG}.base_datasets.asp_sonar_issues
        WHERE record_inserted_by IN ('seed-data', 'seed-data-meridian')
    )
    SELECT org_name, project, type, COUNT(*) AS n
    FROM latest WHERE rk = 1
    GROUP BY org_name, project, type
    ORDER BY org_name, project, type
""").show(50, truncate=False)
