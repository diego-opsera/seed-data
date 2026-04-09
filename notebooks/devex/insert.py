import sys, os, uuid  # uuid used for filter_values_unity row IDs

for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key in ("loader",):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

import yaml
from generators import pull_requests, commits, github_teams_members, itsm_issues

CATALOG = "playground_prod"

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

entities = load_yaml("config/entities.yaml")
story    = load_yaml("config/stories/narrative.yaml")

entities_direct = {**entities, "orgs": [entities["orgs"][1]]}

# ── Part 1: base table data ───────────────────────────────────────────────────
statements = []
statements += [(pull_requests.TABLE,        s) for s in pull_requests.generate(CATALOG, entities_direct, story)]
statements += [(commits.TABLE,              s) for s in commits.generate(CATALOG, entities_direct, story)]
statements += [(github_teams_members.TABLE, s) for s in github_teams_members.generate(CATALOG, entities_direct, story)]
statements += [(itsm_issues.TABLE,          s) for s in itsm_issues.generate(CATALOG, entities_direct, story)]

for i, (table, sql) in enumerate(statements, 1):
    print(f"[{i}/{len(statements)}] {table}...", end=" ")
    spark.sql(sql)
    print("done")

# ── Part 2: devex filter values → shared DORA filter group ───────────────────
# We reuse the filter group created by dora/insert.py so the dashboard's existing
# filter group config picks up devex KPIs automatically.
# Devex filter values use created_by = 'seed-data@devex.io' so devex/delete.py
# can remove them independently without touching DORA filter rows.

fgu_rows = spark.sql(f"""
    SELECT filter_group_id FROM {CATALOG}.master_data.filter_groups_unity
    WHERE createdBy = 'seed-data@demo.io'
""").collect()

if not fgu_rows:
    raise RuntimeError("DORA filter group not found — run dora/insert.py before devex/insert.py")

FILTER_GROUP_ID = fgu_rows[0]["filter_group_id"]
print(f"Using existing DORA filter_group_id: {FILTER_GROUP_ID}")

DEVEX_GITHUB_KPIS = [
    "adca3119-2b97-4163-831d-ce0f3d150c2f",  # developer_throughput_summary_overview
    # commit_statistics
    "9fd5ec78-9fce-49a0-8154-24d3109d3f05",  # commit_statistics_overview
    "af43b1eb-4da0-4197-85fd-d19e146c71a1",  # commit_statistics_sine_wave
    "bd75f3d3-4058-47e0-9d9c-d1864309e166",  # commit_statistics_tab_data_points
    "16d4847d-6dd9-443b-838f-420571223228",  # commit_statistics_table_data
    "33eddd8c-6d0a-415a-be68-bb884ca33ca0",  # commit_to_pr_flow (commit_activity_overview)
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

PIPELINE_STATS_KPIS = [
    "dd3f5cd3-d70d-474c-abb0-f97bf2797e46",  # pipeline_statistics_overview
    "df1f985e-230e-4375-a027-ad3a50827941",  # pipeline_statistics_sine_wave
    "430aa77e-46a8-472f-9fda-18b27a5ee1b9",  # pipeline_statistics_stage_summary
]
PIPELINE_STATS_KPIS_SQL = ", ".join(f"'{k}'" for k in PIPELINE_STATS_KPIS)

DEVEX_JIRA_KPIS = [
    "98c8b001-d968-4083-b2bb-e683c4176fb9",  # feature_delivery_rate
    "317d4bd5-a16d-45ba-92ff-f7632d5ae629",  # story_completion_progress_overview
    "d8be0a56-b6b8-44d0-9771-e42413e752fe",  # issue_completion_progress_overview
]
DEVEX_JIRA_KPIS_SQL = ", ".join(f"'{k}'" for k in DEVEX_JIRA_KPIS)


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
            'null', 'seed-data@devex.io', CURRENT_TIMESTAMP(),
            'seed-data@devex.io', CURRENT_TIMESTAMP(),
            'user', true, {sort_number}
        )
    """)


# GitHub-based devex charts: filter by project_url (commits, PRs, PR size, commit stats)
_fvu(FILTER_GROUP_ID, 'github', 'project_url',
     ['https://github.com/demo-acme-direct/backend',
      'https://github.com/demo-acme-direct/frontend',
      'https://github.com/demo-acme-direct/api-gateway'],
     DEVEX_GITHUB_KPIS_SQL, 0)
print("filter_values_unity: DevEx github filters inserted")

# Pipeline statistics: points to the DORA pipeline_activities project_url
# (pipeline_activities is seeded by dora/ using https://github.com/demo-acme/project_001.git)
_fvu(FILTER_GROUP_ID, 'github', 'project_url',
     ['https://github.com/demo-acme/project_001.git'],
     PIPELINE_STATS_KPIS_SQL, 5)
print("filter_values_unity: Pipeline stats filters inserted")

# Jira-based devex charts: filter by project_name + completion criteria
_fvu(FILTER_GROUP_ID, 'jira', 'project_name',        ['ACME'],                                         DEVEX_JIRA_KPIS_SQL, 1)
_fvu(FILTER_GROUP_ID, 'jira', 'issue_status',        ['Done', 'done', 'Completed'],                    DEVEX_JIRA_KPIS_SQL, 2)
_fvu(FILTER_GROUP_ID, 'jira', 'include_issue_types', ['Story', 'story', 'Bug', 'bug', 'Task', 'task'], DEVEX_JIRA_KPIS_SQL, 3)
print("filter_values_unity: DevEx jira filters inserted")
