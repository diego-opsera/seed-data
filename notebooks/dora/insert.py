import sys, os, uuid

for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key in ("loader",):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

import yaml
from generators import dora, dora_charts

CATALOG = "playground_prod"

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

entities = load_yaml("config/entities.yaml")
story    = load_yaml("config/stories/narrative.yaml")

# ── Part 1: generated base-table data ─────────────────────────────────────────
all_statements = dora.generate(CATALOG, entities, story) + dora_charts.generate(CATALOG, entities, story)

for i, sql in enumerate(all_statements, 1):
    if "pipeline_activities" in sql:
        tbl = "pipeline_activities"
    elif "cfr_mttr_metric_data" in sql:
        tbl = "cfr_mttr_metric_data"
    elif "pipeline_deployment_commits" in sql:
        tbl = "pipeline_deployment_commits"
    elif "mt_itsm_issues_hist" in sql:
        tbl = "mt_itsm_issues_hist"
    elif "mt_itsm_issues_current" in sql:
        tbl = "mt_itsm_issues_current"
    else:
        tbl = sql.split("consumption_layer.")[1].split("\n")[0].strip()
    print(f"[{i}/{len(all_statements)}] {tbl}...", end=" ")
    spark.sql(sql)
    print("done")

# ── Part 2: filter group config (master_data + raw_jira_boards_ci) ─────────────
# These rows are scoped by created_by = 'seed-data@demo.io' for safe deletion.

FGU_ID          = str(uuid.uuid4())
FILTER_GROUP_ID = str(uuid.uuid4())

DF_KPI   = "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9"
LTFC_KPI = "a9337c02-a00e-40ad-9cdc-2d18dfd771c9"
CFR_KPI  = "ab9a59ba-a19c-4358-b195-1648797f77c2"
MTTR_KPI = "906f4f2b-a299-4b24-9a24-2330f45dd493"

# DevEx — github-based charts (filter on project_url)
DEVEX_GITHUB_KPIS = [
    "adca3119-2b97-4163-831d-ce0f3d150c2f",  # developer_throughput_summary_overview
    "33eddd8c-6d0a-415a-be68-bb884ca33ca0",  # commit_statistics_overview (commit_to_pr_flow)
    "dff33190-627a-4b1f-b2e7-fb95a4ebbe00",  # pr_size_area_chart
    "0919c241-9149-494a-8294-8dd4c25ab540",  # pr_size_table_data
    "d049ecd3-3f70-428f-a944-c84bda1fda10",  # pr_size_overview
    "6ddb4873-9bb9-4776-84e0-c74b64cc9fc2",  # pr_size_area_chart (v2)
    "3d7ec1a2-6f0b-4cc2-91ef-ad0f074cccfe",  # pr_size_tab_data_points
]
DEVEX_GITHUB_KPIS_SQL = ", ".join(f"'{k}'" for k in DEVEX_GITHUB_KPIS)

# DevEx — jira-based charts (filter on project_name + issue_status + include_issue_types)
DEVEX_JIRA_KPIS = [
    "98c8b001-d968-4083-b2bb-e683c4176fb9",  # feature_delivery_rate
    "317d4bd5-a16d-45ba-92ff-f7632d5ae629",  # story_completion_progress_overview
    "d8be0a56-b6b8-44d0-9771-e42413e752fe",  # issue_completion_progress_overview
]
DEVEX_JIRA_KPIS_SQL = ", ".join(f"'{k}'" for k in DEVEX_JIRA_KPIS)

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
    _id = str(uuid.uuid4())
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
            'null', 'seed-data@demo.io', CURRENT_TIMESTAMP(),
            'seed-data@demo.io', CURRENT_TIMESTAMP(),
            'user', true, {sort_number}
        )
    """)

# filter_groups_unity — org hierarchy
spark.sql(f"""
    INSERT INTO {CATALOG}.master_data.filter_groups_unity
        (id, level_1, level_2, level_3, level_4, level_5,
         filter_group_id, createdBy, createdAt, updatedBy, updatedAt, active, roles)
    VALUES (
        '{FGU_ID}', 'Acme Corp', '', 'demo-acme-corp', '', '',
        '{FILTER_GROUP_ID}',
        'seed-data@demo.io', CURRENT_TIMESTAMP(),
        'seed-data@demo.io', CURRENT_TIMESTAMP(),
        true, null
    )
""")
print(f"filter_groups_unity: filter_group_id={FILTER_GROUP_ID}")

# DF + LTFC: github filters
_fvu(FILTER_GROUP_ID, 'github', 'project_url',
     ['https://github.com/demo-acme/project_001.git'],
     f"'{DF_KPI}', '{LTFC_KPI}'", 0)
_fvu(FILTER_GROUP_ID, 'github', 'deployment_stages',
     ['deploy'],
     f"'{DF_KPI}', '{LTFC_KPI}'", 1)
_fvu(FILTER_GROUP_ID, 'github', 'pipeline_status_success',
     ['success'],
     f"'{DF_KPI}', '{LTFC_KPI}'", 2)
print("filter_values_unity: DF + LTFC filters inserted")

# CFR + MTTR: jira project name
_fvu(FILTER_GROUP_ID, 'jira', 'project_name',
     ['Acme Platform'],
     f"'{CFR_KPI}', '{MTTR_KPI}'", 3)
print("filter_values_unity: CFR + MTTR filters inserted")

# CTFC: jira filters (board-based, reads v_itsm_issues_hist)
_fvu(FILTER_GROUP_ID, 'jira', 'project_name',        ['ACME'],                                           CTFC_KPIS_SQL, 4)
_fvu(FILTER_GROUP_ID, 'jira', 'issue_status',        ['Done', 'done', 'Completed'],                      CTFC_KPIS_SQL, 5)
_fvu(FILTER_GROUP_ID, 'jira', 'include_issue_types', ['Story', 'story', 'Bug', 'bug', 'Task', 'task'],   CTFC_KPIS_SQL, 6)
_fvu(FILTER_GROUP_ID, 'jira', 'board_ids',           ['1'],                                               CTFC_KPIS_SQL, 7)
_fvu(FILTER_GROUP_ID, 'jira', 'defect_type',         ['Bug', 'bug'],                                      CTFC_KPIS_SQL, 8)
print("filter_values_unity: CTFC filters inserted")

# DevEx: github charts — project_url covers all three demo-acme-direct repos
_fvu(FILTER_GROUP_ID, 'github', 'project_url',
     ['https://github.com/demo-acme-direct/backend',
      'https://github.com/demo-acme-direct/frontend',
      'https://github.com/demo-acme-direct/api-gateway'],
     DEVEX_GITHUB_KPIS_SQL, 9)
print("filter_values_unity: DevEx github filters inserted")

# DevEx: jira charts — project_name + completion status + issue types
_fvu(FILTER_GROUP_ID, 'jira', 'project_name',        ['ACME'],                                          DEVEX_JIRA_KPIS_SQL, 10)
_fvu(FILTER_GROUP_ID, 'jira', 'issue_status',        ['Done', 'done', 'Completed'],                     DEVEX_JIRA_KPIS_SQL, 11)
_fvu(FILTER_GROUP_ID, 'jira', 'include_issue_types', ['Story', 'story', 'Bug', 'bug', 'Task', 'task'],  DEVEX_JIRA_KPIS_SQL, 12)
print("filter_values_unity: DevEx jira filters inserted")

# jira_boards: board_id=1 required by CTFC chart join (raw_jira_boards_ci underlies jira_boards view)
spark.sql(f"""
    INSERT INTO {CATALOG}.source_to_stage.raw_jira_boards_ci
        (message, board_id, board_name, board_type, org_name)
    VALUES (
        '{{"id": 1, "name": "ACME Board", "type": "scrum"}}',
        1, 'ACME Board', 'scrum', 'demo-acme-direct'
    )
""")
print("raw_jira_boards_ci: board_id=1 inserted")
