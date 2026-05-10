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

import sys, os, uuid, yaml

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

# ── Filter wiring ──────────────────────────────────────────────────────────────
# Dashboard SQL all routes through v_filter_group_values_kpi_flattened_unity:
#   filter_groups CTE = SELECT ... FROM v_filter_group_values_kpi_flattened_unity
#                       {{whereClause}}  (e.g. WHERE level_1 = 'Acme Corp')
#   ... JOIN filter_groups ON ot.project_name = mt.project_name
# Without matching filter_values_unity rows, every widget returns zero rows.
#
# We attach our project_name filter values to each demo org's EXISTING
# filter_group_id (created by dora/insert.py — must run before us). Distinct
# created_by tag 'seed-data-cr@demo.io' so delete.py can scope its cleanup.

# KPI UUIDs from vnxt-insights-api-main/src/queries/kpiIdentifierConfig.json
SONAR_RATINGS_KPIS = [
    "9a712182-3c09-44be-ab73-371ed2ef977a",  # sonar_ratings_overview
    "71e0f5f3-13d5-4efd-953f-7f6b4297094f",  # sonar_ratings_table
    "c691a170-151a-4f37-8c70-3237c0fcfca9",  # sonar_ratings_distinct_values
    "eee041d2-a2b5-4100-a360-30418cdd554c",  # coverage_table_data
]
TWISTLOCK_KPIS = [
    "f720f383-ce15-4c65-ae26-32905f87a73f",  # twistlock_security_overview
    "f6633a96-c036-4d3f-ad56-5749966adc9e",  # twistlock_security_tab_points
    "ac98c409-f03f-429e-a125-2b4bdc9fc6dc",  # twistlock_security_table
    "bd98c409-f03f-429e-a125-2b4bdc9fc6dc",  # twistlock_security_filter
]
DEFECT_DENSITY_KPIS = [
    "4605e9d7-5986-478c-aa08-e3d7fa694c48",  # sonarqube_defect_density
]

CR_FILTER_CREATED_BY = "seed-data-cr@demo.io"

# Sonar projects per org — must match what asp_sonar_issues / asp_sonar_measures emit.
# (Generators strip the GitHub org prefix from each repo name to produce these.)
SONAR_PROJECTS = {
    "demo-acme-direct": [r["name"].split("/", 1)[-1] for r in entities_acme["repos"]],
    "demo-meridian":    [r["name"].split("/", 1)[-1] for r in entities_meridian["repos"]],
}

# Each demo org's filter_group_id is owned by dora/insert.py. We look it up
# by the createdBy field rather than hardcoding the UUID (it's regenerated
# every dora run).
ORG_FG_CREATED_BY = {
    "demo-acme-direct": "seed-data@demo.io",
    "demo-meridian":    "seed-data-meridian@demo.io",
}


def _find_filter_group_id(created_by: str):
    rows = spark.sql(f"""
        SELECT filter_group_id FROM {CATALOG}.master_data.filter_groups_unity
        WHERE createdBy = '{created_by}'
        LIMIT 1
    """).collect()
    return rows[0]["filter_group_id"] if rows else None


def _insert_filter_value(filter_group_id, tool_type, filter_name, values, kpi_uuids, sort_number):
    _id = str(uuid.uuid4())
    vals_sql = ", ".join(f"'{v}'" for v in values)
    kpis_sql = ", ".join(f"'{u}'" for u in kpi_uuids)
    spark.sql(f"""
        INSERT INTO {CATALOG}.master_data.filter_values_unity
            (id, filter_group_id, tool_type, filter_name, filter_values, kpi_uuids,
             custom_fieldName, created_by, created_at, updated_by, updated_at,
             source, active, sort_number)
        VALUES (
            '{_id}', '{filter_group_id}',
            '{tool_type}', '{filter_name}',
            array({vals_sql}),
            array({kpis_sql}),
            'null', '{CR_FILTER_CREATED_BY}', CURRENT_TIMESTAMP(),
            '{CR_FILTER_CREATED_BY}', CURRENT_TIMESTAMP(),
            'user', true, {sort_number}
        )
    """)


print(f"\n{'─'*60}\n  FILTER WIRING\n{'─'*60}")

for org_name, projects in SONAR_PROJECTS.items():
    fg_created_by = ORG_FG_CREATED_BY[org_name]
    fg_id = _find_filter_group_id(fg_created_by)
    if not fg_id:
        print(f"  WARNING: no filter_group for {org_name} (createdBy='{fg_created_by}'). "
              f"Did dora/insert.py run first? Skipping.")
        continue
    print(f"  {org_name} → filter_group_id={fg_id}")

    _insert_filter_value(
        fg_id, tool_type='sonar', filter_name='project_name',
        values=projects, kpi_uuids=SONAR_RATINGS_KPIS + DEFECT_DENSITY_KPIS,
        sort_number=20,
    )
    _insert_filter_value(
        fg_id, tool_type='twistlock', filter_name='project_name',
        values=projects, kpi_uuids=TWISTLOCK_KPIS,
        sort_number=21,
    )
    print(f"    inserted 2 filter_values rows (sonar + twistlock) covering {projects}")


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

print(f"\n{'─'*60}\n  VERIFY: filter_values_unity rows we just inserted\n{'─'*60}")
spark.sql(f"""
    SELECT tool_type, filter_name, filter_values, size(kpi_uuids) AS n_kpis, sort_number
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE created_by = '{CR_FILTER_CREATED_BY}'
    ORDER BY tool_type, sort_number
""").show(50, truncate=False)
