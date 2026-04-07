# One-time config inserts for the demo-acme-corp DORA hierarchy.
# NOT wiped by the weekly data refresh — run once per environment.
# Delete with delete_filter_group.py if needed.
# Run via exec(open("/tmp/seed-data/notebooks/dora/insert_filter_group.py").read())

import uuid, json

CATALOG = "playground_prod"

FGU_ID          = str(uuid.uuid4())
FILTER_GROUP_ID = str(uuid.uuid4())

# ── KPI UUIDs (from kpiIdentifierConfig.json) ─────────────────────────────────
DF_KPI   = "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9"
LTFC_KPI = "a9337c02-a00e-40ad-9cdc-2d18dfd771c9"
CFR_KPI  = "ab9a59ba-a19c-4358-b195-1648797f77c2"
MTTR_KPI = "906f4f2b-a299-4b24-9a24-2330f45dd493"
CTFC_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"

# ── 1. filter_groups_unity — hierarchy entry ───────────────────────────────────
# level_3 = 'demo-acme-corp' must match sdm.level in consumption_layer.sdm
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
print(f"filter_groups_unity: id={FGU_ID}, filter_group_id={FILTER_GROUP_ID}")

# ── 2. filter_values_unity — GitHub project URL (feeds DF + LTFC charts) ───────
# project_url must match pipeline_activities.project_url exactly (with .git)
FVU_GITHUB_ID = str(uuid.uuid4())
spark.sql(f"""
    INSERT INTO {CATALOG}.master_data.filter_values_unity
        (id, filter_group_id, tool_type, filter_name, filter_values, kpi_uuids,
         custom_fieldName, created_by, created_at, updated_by, updated_at,
         source, active, sort_number)
    VALUES (
        '{FVU_GITHUB_ID}', '{FILTER_GROUP_ID}',
        'github', 'project_url',
        array('https://github.com/demo-acme/project_001.git'),
        array('{DF_KPI}', '{LTFC_KPI}'),
        'null', 'seed-data@demo.io', CURRENT_TIMESTAMP(),
        'seed-data@demo.io', CURRENT_TIMESTAMP(),
        'user', true, 0
    )
""")
print(f"filter_values_unity (github/project_url): id={FVU_GITHUB_ID}")

# ── 3. filter_values_unity — Jira project name (feeds CFR + MTTR charts) ───────
# project_name must match cfr_mttr_metric_data.issue_project exactly
FVU_JIRA_ID = str(uuid.uuid4())
spark.sql(f"""
    INSERT INTO {CATALOG}.master_data.filter_values_unity
        (id, filter_group_id, tool_type, filter_name, filter_values, kpi_uuids,
         custom_fieldName, created_by, created_at, updated_by, updated_at,
         source, active, sort_number)
    VALUES (
        '{FVU_JIRA_ID}', '{FILTER_GROUP_ID}',
        'jira', 'project_name',
        array('Acme Platform'),
        array('{CFR_KPI}', '{MTTR_KPI}'),
        'null', 'seed-data@demo.io', CURRENT_TIMESTAMP(),
        'seed-data@demo.io', CURRENT_TIMESTAMP(),
        'user', true, 1
    )
""")
# ── 4. filter_values_unity — deployment stages (which step names = a deployment) ─
# deployment_stages must match step_name in pipeline_activities
FVU_STAGES_ID = str(uuid.uuid4())
spark.sql(f"""
    INSERT INTO {CATALOG}.master_data.filter_values_unity
        (id, filter_group_id, tool_type, filter_name, filter_values, kpi_uuids,
         custom_fieldName, created_by, created_at, updated_by, updated_at,
         source, active, sort_number)
    VALUES (
        '{FVU_STAGES_ID}', '{FILTER_GROUP_ID}',
        'github', 'deployment_stages',
        array('deploy'),
        array('{DF_KPI}', '{LTFC_KPI}'),
        'null', 'seed-data@demo.io', CURRENT_TIMESTAMP(),
        'seed-data@demo.io', CURRENT_TIMESTAMP(),
        'user', true, 2
    )
""")
print(f"filter_values_unity (github/deployment_stages): id={FVU_STAGES_ID}")

# ── 5. filter_values_unity — pipeline status success filter ───────────────────
FVU_STATUS_ID = str(uuid.uuid4())
spark.sql(f"""
    INSERT INTO {CATALOG}.master_data.filter_values_unity
        (id, filter_group_id, tool_type, filter_name, filter_values, kpi_uuids,
         custom_fieldName, created_by, created_at, updated_by, updated_at,
         source, active, sort_number)
    VALUES (
        '{FVU_STATUS_ID}', '{FILTER_GROUP_ID}',
        'github', 'pipeline_status_success',
        array('success'),
        array('{DF_KPI}', '{LTFC_KPI}'),
        'null', 'seed-data@demo.io', CURRENT_TIMESTAMP(),
        'seed-data@demo.io', CURRENT_TIMESTAMP(),
        'user', true, 3
    )
""")
print(f"filter_values_unity (github/pipeline_status_success): id={FVU_STATUS_ID}")

# ── 6. filter_values_unity — CTFC Jira filters (Cycle Time for Changes) ────────
# CTFC reads from v_itsm_issues_current (Jira issues), not pipeline data.
# Filters: project_name, issue_status, include_issue_types, board_ids, defect_type.
_ctfc_filters = [
    ("project_name",        ["ACME"],                                           4),
    ("issue_status",        ["Done", "done", "Completed"],                      5),
    ("include_issue_types", ["Story", "story", "Bug", "bug", "Task", "task"],   6),
    ("board_ids",           ["1"],                                               7),
    ("defect_type",         ["Bug", "bug"],                                      8),
]
for _fname, _fvals, _sort in _ctfc_filters:
    _id = str(uuid.uuid4())
    _vals_sql = ", ".join(f"'{v}'" for v in _fvals)
    spark.sql(f"""
        INSERT INTO {CATALOG}.master_data.filter_values_unity
            (id, filter_group_id, tool_type, filter_name, filter_values, kpi_uuids,
             custom_fieldName, created_by, created_at, updated_by, updated_at,
             source, active, sort_number)
        VALUES (
            '{_id}', '{FILTER_GROUP_ID}',
            'jira', '{_fname}',
            array({_vals_sql}),
            array('{CTFC_KPI}'),
            'null', 'seed-data@demo.io', CURRENT_TIMESTAMP(),
            'seed-data@demo.io', CURRENT_TIMESTAMP(),
            'user', true, {_sort}
        )
    """)
    print(f"filter_values_unity (jira/{_fname} CTFC): id={_id}")

print(json.dumps({
    "filter_group_id": FILTER_GROUP_ID,
    "hierarchy": "Acme Corp > demo-acme-corp",
    "github_url": "https://github.com/demo-acme/project_001.git",
    "jira_project_cfr_mttr": "Acme Platform",
    "jira_project_ctfc": "ACME",
    "kpis": {"df": DF_KPI, "ltfc": LTFC_KPI, "cfr": CFR_KPI, "mttr": MTTR_KPI, "ctfc": CTFC_KPI}
}))
