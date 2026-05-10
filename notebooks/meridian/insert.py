import sys, os, uuid, yaml

# Module cache-busting — ensures fresh generator code on re-run
for _key in list(sys.modules.keys()):
    if _key.startswith("generators"):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

from generators import (
    dora_meridian,
    devex_meridian,
    cr_meridian,
    release_mgmt_meridian,
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

# ── Part 2b: DevEx (commits + PRs) ────────────────────────────────────────────
devex_result = devex_meridian.generate(CATALOG, entities_meridian, meridian_story)
devex_statements = (
    [("commits_rest_api", s) for s in devex_result["commits"]] +
    [("pull_requests",    s) for s in devex_result["prs"]]
)
for i, (table, sql) in enumerate(devex_statements, 1):
    print(f"[DevEx {i}/{len(devex_statements)}] {table}...", end=" ")
    spark.sql(sql)
    print("done")

# ── Part 2c: Change Requests ───────────────────────────────────────────────────
cr_statements = cr_meridian.generate(CATALOG, entities_meridian, meridian_story)
for i, sql in enumerate(cr_statements, 1):
    print(f"[CR {i}/{len(cr_statements)}] trf_servicenow_change_requests...", end=" ")
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

# DevEx GitHub charts: commit_statistics, pr_size, pull_request_statistics, developer_throughput
DEVEX_GITHUB_KPIS = [
    "adca3119-2b97-4163-831d-ce0f3d150c2f",  # developer_throughput_summary_overview
    # commit_statistics
    "9fd5ec78-9fce-49a0-8154-24d3109d3f05",  # commit_statistics_overview
    "af43b1eb-4da0-4197-85fd-d19e146c71a1",  # commit_statistics_sine_wave
    "bd75f3d3-4058-47e0-9d9c-d1864309e166",  # commit_statistics_tab_data_points
    "16d4847d-6dd9-443b-838f-420571223228",  # commit_statistics_table_data
    "33eddd8c-6d0a-415a-be68-bb884ca33ca0",  # commit_to_pr_flow
    # pr_size
    "dff33190-627a-4b1f-b2e7-fb95a4ebbe00",  # pr_size_area_chart
    "0919c241-9149-494a-8294-8dd4c25ab540",  # pr_size_table_data
    "d049ecd3-3f70-428f-a944-c84bda1fda10",  # pr_size_overview
    "6ddb4873-9bb9-4776-84e0-c74b64cc9fc2",  # pr_size_area_chart (v2)
    "3d7ec1a2-6f0b-4cc2-91ef-ad0f074cccfe",  # pr_size_tab_data_points
    # pull_request_statistics
    "62caa741-e5ed-4e7a-8698-cbd7d8a2e042",  # pull_request_statistics_overview
    "9637286b-a5c2-4428-ac3b-9c207b8ad722",  # pull_request_statistics_sine_wave
    "745c5458-56a1-40b1-85b1-81e3fc86d119",  # pull_request_statistics_tab_data
    "fa10f775-0a32-44e4-bab4-c986a70bc563",  # pull_request_statistics_table_data
]
DEVEX_GITHUB_KPIS_SQL = ", ".join(f"'{k}'" for k in DEVEX_GITHUB_KPIS)

# SPACE charts (commits_vs_space, pr_size_vs_performance, etc.)
SPACE_GITHUB_KPIS = [
    "space_c3d4e5f6-g7h8-9012-cdef-345678901234",  # space_devex_metrics
    "space_d4e5f6g7-h8i9-0123-defg-456789012345",  # commits_vs_space
    "space_e5f6g7h8-i9j0-1234-efgh-567890123456",  # pr_size_vs_performance
    "space_f6g7h8i9-j0k1-2345-fghi-678901234567",  # space_pr_metrics
    "space_g7h8i9j0-k1l2-3456-ghij-789012345678",  # space_activity_patterns
    "space_h8i9j0k1-l2m3-4567-hijk-890123456789",  # space_commit_patterns
]
SPACE_GITHUB_KPIS_SQL = ", ".join(f"'{k}'" for k in SPACE_GITHUB_KPIS)

# Pipeline statistics: points to the Meridian pipeline_activities project_url
PIPELINE_STATS_KPIS = [
    "dd3f5cd3-d70d-474c-abb0-f97bf2797e46",  # pipeline_statistics_overview
    "df1f985e-230e-4375-a027-ad3a50827941",  # pipeline_statistics_sine_wave
    "430aa77e-46a8-472f-9fda-18b27a5ee1b9",  # pipeline_statistics_stage_summary
]
PIPELINE_STATS_KPIS_SQL = ", ".join(f"'{k}'" for k in PIPELINE_STATS_KPIS)

_fvu(FILTER_GROUP_ID, 'github', 'project_url',
     ['https://github.com/demo-meridian/data-platform'],
     DEVEX_GITHUB_KPIS_SQL, 9)
print("filter_values_unity: DevEx github filters inserted")

_fvu(FILTER_GROUP_ID, 'github', 'project_url',
     ['https://github.com/demo-meridian/data-platform'],
     SPACE_GITHUB_KPIS_SQL, 10)
print("filter_values_unity: SPACE github filters inserted")

_fvu(FILTER_GROUP_ID, 'github', 'project_url',
     ['https://github.com/demo-meridian/data-platform.git'],
     PIPELINE_STATS_KPIS_SQL, 11)
print("filter_values_unity: Pipeline stats filters inserted")

# Change Request: assignment_groups matches trf_servicenow_change_requests.issue_project
CHANGE_REQUEST_KPIS = [
    "4791045c-5bb4-4745-8c2f-d68e5783da0a",  # change_request_overview
    "f3f7946a-3153-419f-9ce8-ab8117cdc395",  # change_request_chart_data (sine)
    "255e537f-7572-461b-be48-b46ead7e5d70",  # change_request_summary_block
    "ec56bcb4-86ca-413c-bb57-bb229483658a",  # change_request_table_data
    "fc56bcb4-86ca-413c-bb57-bb229483658a",  # change_request_filters_data
]
CHANGE_REQUEST_KPIS_SQL = ", ".join(f"'{k}'" for k in CHANGE_REQUEST_KPIS)
_fvu(FILTER_GROUP_ID, 'servicenow', 'assignment_groups',
     [cr_meridian.ASSIGNMENT_GROUP],
     CHANGE_REQUEST_KPIS_SQL, 12)
print("filter_values_unity: Change Request filters inserted")

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

# ── Part 6: Release Management ─────────────────────────────────────────────────
# 5 releases: 2 pre-Opsera (quarterly batch), 3 post-Opsera (continuous).
# Numbers calibrated to look consistent with DORA/DevEx data above.

rm_statements = release_mgmt_meridian.generate(CATALOG, entities_meridian, meridian_story)
for i, sql in enumerate(rm_statements, 1):
    print(f"[Release {i}/{len(rm_statements)}] release_management_detail...", end=" ")
    spark.sql(sql)
    print("done")
