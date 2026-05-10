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

from generators.utils import load_story

from generators import dependabot_scan_alert, asp_sonar_issues, asp_sonar_measures, twistlock_security_issues, invicti_was, git_custodian, junit_insights

# Ensure the missing raw_invicti_all_issues + gitscraper tables exist before
# we try to INSERT. Idempotent CREATE TABLE IF NOT EXISTS — safe to re-run.
exec(open("/tmp/seed-data/notebooks/code_reliability/create_table_invicti_issues.py").read())
exec(open("/tmp/seed-data/notebooks/code_reliability/create_table_gitscraper.py").read())

CATALOG = "playground_prod"

# ── Story configs ──────────────────────────────────────────────────────────────
# Both stories share the same date window (rewritten daily by master insert.py).

acme_story     = load_story("narrative")
meridian_story = load_story("meridian_narrative")

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
    ("dependabot_scan_alert",     dependabot_scan_alert),
    ("asp_sonar_issues",          asp_sonar_issues),
    ("asp_sonar_measures",        asp_sonar_measures),
    ("twistlock_security_issues", twistlock_security_issues),
    ("invicti_was",               invicti_was),
    ("git_custodian",             git_custodian),
    ("junit_insights",            junit_insights),
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

# KPI UUIDs — pulled dynamically from master_data.kpi_table so we cover every
# variant the dashboard widget might be bound to (template-common, template-hw,
# wd_hier_*, opsera_*, custom user-created). The filter_groups SQL doesn't
# enforce tool_type during the project_name JOIN, so binning is loose:
#   anything matching sonar/coverage/reliability/defect/quality → 'sonar' bucket
#   anything matching twistlock                                  → 'twistlock' bucket
#   anything matching web/invicti/was                            → 'was' bucket
#
# Hardcoded fallback lists (kpiIdentifierConfig.json) are kept as a safety
# net in case kpi_table is empty or the dynamic query fails.
SONAR_RATINGS_FALLBACK = [
    "9a712182-3c09-44be-ab73-371ed2ef977a",
    "71e0f5f3-13d5-4efd-953f-7f6b4297094f",
    "c691a170-151a-4f37-8c70-3237c0fcfca9",
    "eee041d2-a2b5-4100-a360-30418cdd554c",
    "4605e9d7-5986-478c-aa08-e3d7fa694c48",
]
TWISTLOCK_FALLBACK = [
    "f720f383-ce15-4c65-ae26-32905f87a73f",
    "f6633a96-c036-4d3f-ad56-5749966adc9e",
    "ac98c409-f03f-429e-a125-2b4bdc9fc6dc",
    "bd98c409-f03f-429e-a125-2b4bdc9fc6dc",
]


def _kpi_uuids_by_pattern(rlike_pattern: str) -> list:
    """Pull every kpi_table UUID whose displayName or kpi_identifier matches."""
    try:
        rows = spark.sql(f"""
            SELECT DISTINCT uuid
            FROM {CATALOG}.master_data.kpi_table
            WHERE LOWER(COALESCE(displayName, '')) RLIKE '{rlike_pattern}'
               OR LOWER(COALESCE(kpi_identifier, '')) RLIKE '{rlike_pattern}'
        """).collect()
        return sorted({r["uuid"] for r in rows if r["uuid"]})
    except Exception as e:
        print(f"  WARN: kpi_table query failed: {e}")
        return []


SONAR_KPIS = _kpi_uuids_by_pattern("sonar|coverage|reliability|defect|quality_gate|maintainability") \
             or SONAR_RATINGS_FALLBACK
TWISTLOCK_KPIS = _kpi_uuids_by_pattern("twistlock|container") or TWISTLOCK_FALLBACK
WAS_KPIS = _kpi_uuids_by_pattern("invicti|web_app_security|was_overview|web app security|web_app|web-app")
GIT_CUSTODIAN_KPIS = _kpi_uuids_by_pattern("git custodian|gitcustodian|gitscraper|git_custodian")
# Drill-down sub-KPIs (e.g. junit_insights_drilldown_table_data,
# junit_insights_tab_data_points) only live in kpiIdentifierConfig.json
# on the FE — not in master_data.kpi_table — so dynamic discovery misses
# them. Hardcode here.
JUNIT_FALLBACK_DRILLDOWN = [
    "31b68fd2-aaf6-4eac-9638-c14dc5c2ebf6",  # junit_insights_drilldown_table_data
    "e4d0bff4-392a-4878-9b78-522f0557c31a",  # junit_insights_tab_data_points
]
JUNIT_KPIS = sorted(set(_kpi_uuids_by_pattern("junit")) | set(JUNIT_FALLBACK_DRILLDOWN))

print(f"  Discovered {len(SONAR_KPIS)} sonar/coverage/defect KPI UUIDs")
print(f"  Discovered {len(TWISTLOCK_KPIS)} twistlock KPI UUIDs")
print(f"  Discovered {len(WAS_KPIS)} web app security KPI UUIDs")
print(f"  Discovered {len(GIT_CUSTODIAN_KPIS)} git custodian KPI UUIDs")
print(f"  Discovered {len(JUNIT_KPIS)} junit KPI UUIDs")

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

    if SONAR_KPIS:
        _insert_filter_value(
            fg_id, tool_type='sonar', filter_name='project_name',
            values=projects, kpi_uuids=SONAR_KPIS,
            sort_number=20,
        )
    if TWISTLOCK_KPIS:
        _insert_filter_value(
            fg_id, tool_type='twistlock', filter_name='project_name',
            values=projects, kpi_uuids=TWISTLOCK_KPIS,
            sort_number=21,
        )
    if WAS_KPIS:
        _insert_filter_value(
            fg_id, tool_type='was', filter_name='project_name',
            values=projects, kpi_uuids=WAS_KPIS,
            sort_number=22,
        )
    # Git Custodian filters by pipeline_name / branch_name (NOT project_name).
    # gc_overview.sql JOIN: s.pipelineName = f.pipeline_name OR s.branch = f.branch_name
    # Match the synthesized pipeline_name our git_custodian generator emits:
    # f"{repository}-pipeline" where repository is the post-slash part of repo name.
    if GIT_CUSTODIAN_KPIS:
        gc_pipelines = [f"{p}-pipeline" for p in projects]
        _insert_filter_value(
            fg_id, tool_type='git_custodian', filter_name='pipeline_name',
            values=gc_pipelines, kpi_uuids=GIT_CUSTODIAN_KPIS,
            sort_number=23,
        )
        _insert_filter_value(
            fg_id, tool_type='git_custodian', filter_name='branch_name',
            values=['main'], kpi_uuids=GIT_CUSTODIAN_KPIS,
            sort_number=24,
        )
    # JUnit Insights filters by project_url (matches git_url in the source row).
    # MUST end with .git — that's what the flattened view surfaces (DORA wires
    # .git URLs and the view unions across the filter_group). The junit SQL
    # does an exact string match on git_url = project_url, so generator and
    # filter must agree.
    if JUNIT_KPIS:
        _entities_for_org = {
            "demo-acme-direct": entities_acme,
            "demo-meridian":    entities_meridian,
        }[org_name]
        junit_urls = []
        for r in _entities_for_org["repos"]:
            u = (r.get("html_url") or f"https://github.com/{r['name']}").rstrip("/")
            if not u.endswith(".git"):
                u = u + ".git"
            junit_urls.append(u)
        _insert_filter_value(
            fg_id, tool_type='github', filter_name='project_url',
            values=junit_urls, kpi_uuids=JUNIT_KPIS,
            sort_number=25,
        )
    n_inserted = sum(bool(x) for x in [SONAR_KPIS, TWISTLOCK_KPIS, WAS_KPIS, GIT_CUSTODIAN_KPIS, JUNIT_KPIS])
    print(f"    inserted {n_inserted}+ filter_values rows covering {projects}")


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

print(f"\n{'─'*60}\n  VERIFY: asp_sonar_measures (latest scan per project per org)\n{'─'*60}")
spark.sql(f"""
    WITH latest AS (
        SELECT org_name, project_name, branch,
               project_coverage_value, project_reliability_rating_value,
               project_security_rating_value, project_sqale_rating_value,
               project_bugs_value, quality_gate_status,
               source_record_insert_datetime,
               row_number() OVER (PARTITION BY org_name, project_name, branch
                                  ORDER BY source_record_insert_datetime DESC) AS rk
        FROM {CATALOG}.source_to_stage.raw_sonar_metric_split_data_branchwise
        WHERE record_inserted_by IN ('seed-data', 'seed-data-meridian')
    )
    SELECT org_name, project_name,
           project_coverage_value AS coverage_pct,
           project_reliability_rating_value AS rel_rating,
           project_security_rating_value    AS sec_rating,
           project_sqale_rating_value       AS maint_rating,
           project_bugs_value               AS bugs,
           quality_gate_status              AS gate
    FROM latest WHERE rk = 1
    ORDER BY org_name, project_name
""").show(50, truncate=False)

print(f"\n{'─'*60}\n  VERIFY: measures⋈issues JOIN key alignment (must be > 0)\n{'─'*60}")
spark.sql(f"""
    SELECT m.org_name, m.project_name, COUNT(*) AS n_matches
    FROM {CATALOG}.source_to_stage.raw_sonar_metric_split_data_branchwise m
    INNER JOIN {CATALOG}.source_to_stage.raw_sonar_type_data_branchwise i
        ON m.org_name = i.org_name
       AND m.project_name = i.project
       AND m.branch = i.branch
       AND m.source_record_insert_datetime = i.source_record_insert_datetime
    WHERE m.record_inserted_by IN ('seed-data', 'seed-data-meridian')
    GROUP BY m.org_name, m.project_name
    ORDER BY m.org_name, m.project_name
""").show(50, truncate=False)

print(f"\n{'─'*60}\n  VERIFY: junit_test_suite_report by repo\n{'─'*60}")
spark.sql(f"""
    SELECT repository, COUNT(*) AS n_runs,
           SUM(passed_tests) AS total_passed,
           SUM(failed_tests) AS total_failed,
           SUM(errored_tests) AS total_errored,
           SUM(skipped_tests) AS total_skipped
    FROM {CATALOG}.source_to_stage.junit_test_suite_report
    WHERE service_principal IN ('seed-data', 'seed-data-meridian')
    GROUP BY repository
    ORDER BY repository
""").show(50, truncate=False)

print(f"\n{'─'*60}\n  VERIFY: git custodian scans + issues per repo\n{'─'*60}")
spark.sql(f"""
    SELECT s.repository, s.pipelineName,
           COUNT(DISTINCT s._id)        AS n_scans,
           SUM(s.totalIssues)           AS sum_total_issues,
           COUNT(i._id)                 AS n_issue_rows
    FROM {CATALOG}.source_to_stage.raw_mongo_transformed_data_gitscraper s
    LEFT JOIN {CATALOG}.source_to_stage.raw_mongo_transformed_data_gitscraper_issues i
        ON s._id = i._id
    WHERE s.record_inserted_by IN ('seed-data', 'seed-data-meridian')
    GROUP BY s.repository, s.pipelineName
    ORDER BY s.repository
""").show(50, truncate=False)

print(f"\n{'─'*60}\n  VERIFY: raw_invicti_data scans + severity\n{'─'*60}")
spark.sql(f"""
    SELECT WebsiteName, ThreatLevel,
           VulnerabilityCriticalCount AS crit,
           VulnerabilityHighCount AS high,
           VulnerabilityMediumCount AS med,
           VulnerabilityLowCount AS low,
           InitiatedAt
    FROM {CATALOG}.source_to_stage.raw_invicti_data
    WHERE record_inserted_by IN ('seed-data', 'seed-data-meridian')
    ORDER BY WebsiteName
""").show(50, truncate=False)

print(f"\n{'─'*60}\n  VERIFY: raw_invicti_all_issues by website + severity\n{'─'*60}")
spark.sql(f"""
    SELECT WebsiteName, Severity, COUNT(*) AS n
    FROM {CATALOG}.source_to_stage.raw_invicti_all_issues
    WHERE record_inserted_by IN ('seed-data', 'seed-data-meridian')
    GROUP BY WebsiteName, Severity
    ORDER BY WebsiteName, Severity
""").show(50, truncate=False)

print(f"\n{'─'*60}\n  VERIFY: twistlock_security_issues by project + severity\n{'─'*60}")
spark.sql(f"""
    SELECT project_name, severity, COUNT(*) AS n_components,
           SUM(size(cve)) AS n_cves
    FROM {CATALOG}.base_datasets.twistlock_security_issues
    WHERE record_inserted_by IN ('seed-data', 'seed-data-meridian')
    GROUP BY project_name, severity
    ORDER BY project_name, severity
""").show(50, truncate=False)

print(f"\n{'─'*60}\n  VERIFY: kpi_table UUIDs we discovered + bound to our filter values\n{'─'*60}")
all_kpis = SONAR_KPIS + TWISTLOCK_KPIS + WAS_KPIS
if all_kpis:
    in_clause = ", ".join(f"'{u}'" for u in all_kpis)
    spark.sql(f"""
        SELECT kt.uuid, kt.displayName, kt.kpi_identifier
        FROM {CATALOG}.master_data.kpi_table kt
        WHERE kt.uuid IN ({in_clause})
        ORDER BY kt.displayName
    """).show(200, truncate=False)

print(f"\n{'─'*60}\n  VERIFY: filter_values_unity rows we just inserted\n{'─'*60}")
spark.sql(f"""
    SELECT tool_type, filter_name, filter_values, size(kpi_uuids) AS n_kpis, sort_number
    FROM {CATALOG}.master_data.filter_values_unity
    WHERE created_by = '{CR_FILTER_CREATED_BY}'
    ORDER BY tool_type, sort_number
""").show(50, truncate=False)
