import sys, os, uuid, yaml

# Module cache-busting — ensures fresh generator code on re-run
for _key in list(sys.modules.keys()):
    if _key.startswith("generators"):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

from generators import (
    dora_meridian,
    direct_data, seats_usage, org_mapping,
    ide_org_level, ai_assistant_acceptance, ai_usage_user_level,
    copilot_billing, code_scan_alert, secret_scan_alert,
)

CATALOG = "playground_prod"

# ── Story configs ──────────────────────────────────────────────────────────────
# narrative.yaml drives the rolling date window (rewritten daily by master insert).
# meridian_narrative.yaml holds Meridian-specific settings; it inherits the date
# range from narrative.yaml so both stories always share the same window.

narrative = yaml.safe_load(open("config/stories/narrative.yaml"))
meridian_story = yaml.safe_load(open("config/stories/meridian_narrative.yaml"))
meridian_story["start_date"] = narrative["start_date"]
meridian_story["end_date"]   = narrative["end_date"]

# ── Entities ───────────────────────────────────────────────────────────────────
# Defined inline so we never touch config/entities.yaml (Acme-scoped).
# Both orgs[0] and orgs[1] resolve to demo-meridian — seats_usage and org_mapping
# read orgs[1], all other direct generators read orgs[0].

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
    # Python-only language list: all generated users get python (100% for a PySpark/SQL team)
    "languages": ["python"],
    "repos": [
        {"name": "demo-meridian/data-platform", "html_url": "https://github.com/demo-meridian/data-platform"},
    ],
    "ides":     ["vscode", "intellij", "visualstudio", "eclipse", "xcode"],
    "features": ["code_completion", "chat_panel_ask_mode", "chat_panel_edit_mode", "agent_edit"],
    "models":   ["gpt-4o", "gpt-4o-mini", "claude-3.7-sonnet", "claude-4.0-sonnet", "claude-4.5-sonnet", "o3-mini"],
}


# ── Part 1: DORA base-table data ───────────────────────────────────────────────

dora_statements = dora_meridian.generate(CATALOG, entities_meridian, meridian_story)

for i, sql in enumerate(dora_statements, 1):
    if "pipeline_activities" in sql:
        tbl = "pipeline_activities"
    elif "pipeline_deployment_commits" in sql:
        tbl = "pipeline_deployment_commits"
    elif "cfr_mttr_metric_data" in sql:
        tbl = "cfr_mttr_metric_data"
    elif "mt_itsm_issues_hist" in sql:
        tbl = "mt_itsm_issues_hist"
    elif "mt_itsm_issues_current" in sql:
        tbl = "mt_itsm_issues_current"
    else:
        tbl = sql.split("\n")[0].strip()
    print(f"[DORA {i}/{len(dora_statements)}] {tbl}...", end=" ")
    spark.sql(sql)
    print("done")

# ── Part 2: Direct / Copilot base-table data ───────────────────────────────────
# Same generator set as direct/insert.py, scoped to org_name = 'demo-meridian'.
# Note: seats_usage and copilot_billing have hardcoded Acme-scale seat allocations
# (45–150); the active-seat numbers will be correct (12-person team) but total
# allocated will be overstated. Acceptable for demo purposes.
# Excluded: file_extensions (shared MERGE table — idempotent, no-op if rows exist).

direct_statements = []
direct_statements += [(direct_data.TABLE,             s) for s in direct_data.generate(CATALOG, entities_meridian, meridian_story)]
direct_statements += [(seats_usage.TABLE,             s) for s in seats_usage.generate(CATALOG, entities_meridian, meridian_story)]
direct_statements += [(org_mapping.TABLE,             s) for s in org_mapping.generate(CATALOG, entities_meridian, meridian_story)]
direct_statements += [(ide_org_level.TABLE,           s) for s in ide_org_level.generate(CATALOG, entities_meridian, meridian_story)]
direct_statements += [(ai_assistant_acceptance.TABLE, s) for s in ai_assistant_acceptance.generate(CATALOG, entities_meridian, meridian_story)]
direct_statements += [(ai_usage_user_level.TABLE,     s) for s in ai_usage_user_level.generate(CATALOG, entities_meridian, meridian_story)]
direct_statements += [(copilot_billing.TABLE,         s) for s in copilot_billing.generate(CATALOG, entities_meridian, meridian_story)]
direct_statements += [(code_scan_alert.TABLE,         s) for s in code_scan_alert.generate(CATALOG, entities_meridian, meridian_story)]
direct_statements += [(secret_scan_alert.TABLE,       s) for s in secret_scan_alert.generate(CATALOG, entities_meridian, meridian_story)]

for i, (table, sql) in enumerate(direct_statements, 1):
    print(f"[Direct {i}/{len(direct_statements)}] {table}...", end=" ")
    spark.sql(sql)
    print("done")

# ── Part 3: filter_groups_unity ────────────────────────────────────────────────
# Distinct createdBy = 'seed-data-meridian@demo.io' so delete.py can scope
# independently of Acme rows.

FGU_ID          = str(uuid.uuid4())
FILTER_GROUP_ID = str(uuid.uuid4())

CREATED_BY = "seed-data-meridian@demo.io"

# KPI UUIDs — same as dora/insert.py (shared KPI config, different filter group)
DF_KPI   = "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9"
LTFC_KPI = "a9337c02-a00e-40ad-9cdc-2d18dfd771c9"
CFR_KPI  = "ab9a59ba-a19c-4358-b195-1648797f77c2"
MTTR_KPI = "906f4f2b-a299-4b24-9a24-2330f45dd493"

CTFC_KPIS = [
    "f60d8a58-7c8d-4dd6-9b54-6c07715ae5ec",  # ctfc_custom_overview
    "7f0d028a-06fd-4c1d-8915-3f94c53899e2",  # ctfc_custom_sine_wave
    "c03790e5-874b-4c64-bae9-3f6e21e5b42e",  # ctfc_custom_compare_chart
    "26e4b366-78a3-4d2d-b1d6-afcf93db8269",  # ctfc_custom_tab_data_points
    "8b81e5db-9b51-43fc-bf9a-6fa78a3b5f89",  # ctfc_custom_table_data
    "6b81e5db-9b51-43fc-bf9a-6fa78a3b5f89",  # ctfc_custom_table_data (alt)
]
CTFC_KPIS_SQL = ", ".join(f"'{k}'" for k in CTFC_KPIS)


def _fvu(filter_group_id, tool_type, filter_name, filter_values, kpi_uuids_sql, sort_number):
    _id   = str(uuid.uuid4())
    _vals = ", ".join(f"'{v}'" for v in filter_values)
    spark.sql(f"""
        INSERT INTO {CATALOG}.master_data.filter_values_unity
            (id, filter_group_id, tool_type, filter_name, filter_values, kpi_uuids,
             custom_fieldName, created_by, created_at, updated_by, updated_at,
             source, active, sort_number)
        VALUES (
            '{_id}', '{filter_group_id}',
            '{tool_type}', '{filter_name}',
            array({_vals}),
            array({kpi_uuids_sql}),
            'null', '{CREATED_BY}', CURRENT_TIMESTAMP(),
            '{CREATED_BY}', CURRENT_TIMESTAMP(),
            'user', true, {sort_number}
        )
    """)


spark.sql(f"""
    INSERT INTO {CATALOG}.master_data.filter_groups_unity
        (id, level_1, level_2, level_3, level_4, level_5,
         filter_group_id, createdBy, createdAt, updatedBy, updatedAt, active, roles)
    VALUES (
        '{FGU_ID}', 'Meridian Analytics', '', 'demo-meridian', '', '',
        '{FILTER_GROUP_ID}',
        '{CREATED_BY}', CURRENT_TIMESTAMP(),
        '{CREATED_BY}', CURRENT_TIMESTAMP(),
        true, null
    )
""")
print(f"filter_groups_unity: filter_group_id={FILTER_GROUP_ID}")

# ── Part 4: filter_values_unity ────────────────────────────────────────────────

# DF + LTFC: github project filters
_fvu(FILTER_GROUP_ID, 'github', 'project_url',
     ['https://github.com/demo-meridian/data-platform.git'],
     f"'{DF_KPI}', '{LTFC_KPI}'", 0)
_fvu(FILTER_GROUP_ID, 'github', 'deployment_stages',
     ['deploy'],
     f"'{DF_KPI}', '{LTFC_KPI}'", 1)
_fvu(FILTER_GROUP_ID, 'github', 'pipeline_status_success',
     ['success'],
     f"'{DF_KPI}', '{LTFC_KPI}'", 2)
print("filter_values_unity: DF + LTFC filters inserted")

# CFR + MTTR: jira project name (matches issue_project in cfr_mttr_metric_data)
_fvu(FILTER_GROUP_ID, 'jira', 'project_name',
     ['Meridian Data Platform'],
     f"'{CFR_KPI}', '{MTTR_KPI}'", 3)
print("filter_values_unity: CFR + MTTR filters inserted")

# CTFC: jira promotion ticket filters (board_id=2 — must not collide with Acme's board_id=1)
# project_name matches issue_project = 'MDP' in mt_itsm_issues_hist
_fvu(FILTER_GROUP_ID, 'jira', 'project_name',
     ['MDP'],
     CTFC_KPIS_SQL, 4)
_fvu(FILTER_GROUP_ID, 'jira', 'issue_status',
     ['Done', 'done', 'Completed'],
     CTFC_KPIS_SQL, 5)
_fvu(FILTER_GROUP_ID, 'jira', 'include_issue_types',
     ['Story', 'story', 'Bug', 'bug', 'Task', 'task'],
     CTFC_KPIS_SQL, 6)
_fvu(FILTER_GROUP_ID, 'jira', 'board_ids',
     ['2'],
     CTFC_KPIS_SQL, 7)
_fvu(FILTER_GROUP_ID, 'jira', 'defect_type',
     ['Bug', 'bug'],
     CTFC_KPIS_SQL, 8)
print("filter_values_unity: CTFC filters inserted")

# ── Part 5: raw_jira_boards_ci ─────────────────────────────────────────────────
# board_id=2 is required by the CTFC chart join (underlies the jira_boards view).
# Acme uses board_id=1 — these must not collide.

spark.sql(f"""
    INSERT INTO {CATALOG}.source_to_stage.raw_jira_boards_ci
        (message, board_id, board_name, board_type, org_name)
    VALUES (
        '{{"id": 2, "name": "MDP Board", "type": "scrum"}}',
        2, 'MDP Board', 'scrum', 'demo-meridian'
    )
""")
print("raw_jira_boards_ci: board_id=2 inserted")
