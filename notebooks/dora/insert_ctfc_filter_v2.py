# Patch v2: replace wrong CTFC filter rows (0cb8981e = jira_velocity, not CTFC)
# with correct rows using all actual ctfc_custom_* KPI UUIDs from kpiIdentifierConfig.json.
# Run ONCE in environments where insert_ctfc_filter.py already ran with the wrong UUID.
# Run via exec(open("/tmp/seed-data/notebooks/dora/insert_ctfc_filter_v2.py").read())

import uuid as _uuid, json

CATALOG = "playground_prod"
FILTER_GROUP_ID = "d277535f-a8cb-4429-965d-a9de685b4045"  # demo-acme-corp

# Wrong UUID we used before (0cb8981e = jira_velocity_table_data, not CTFC)
WRONG_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"

# All correct CTFC chart UUIDs (from kpiIdentifierConfig.json: ctfc_custom_*)
CTFC_KPIS = [
    "f60d8a58-7c8d-4dd6-9b54-6c07715ae5ec",  # ctfc_custom_overview
    "7f0d028a-06fd-4c1d-8915-3f94c53899e2",  # ctfc_custom_sine_wave
    "c03790e5-874b-4c64-bae9-3f6e21e5b42e",  # ctfc_custom_compare_chart
    "26e4b366-78a3-4d2d-b1d6-afcf93db8269",  # ctfc_custom_tab_data_points
    "8b81e5db-9b51-43fc-bf9a-6fa78a3b5f89",  # ctfc_custom_table_data
    "6b81e5db-9b51-43fc-bf9a-6fa78a3b5f89",  # ctfc_custom_table_data (alt)
]
KPIS_SQL = ", ".join(f"'{k}'" for k in CTFC_KPIS)

# ── 1. Delete wrong filter rows ───────────────────────────────────────────────
spark.sql(f"""
    DELETE FROM {CATALOG}.master_data.filter_values_unity
    WHERE filter_group_id = '{FILTER_GROUP_ID}'
      AND array_contains(kpi_uuids, '{WRONG_KPI}')
      AND created_by = 'seed-data@demo.io'
""")
print(f"Deleted wrong filter rows for {WRONG_KPI}")

# ── 2. Insert correct CTFC filter rows ────────────────────────────────────────
_ctfc_filters = [
    ("project_name",        ["ACME"],                                           4),
    ("issue_status",        ["Done", "done", "Completed"],                      5),
    ("include_issue_types", ["Story", "story", "Bug", "bug", "Task", "task"],   6),
    ("board_ids",           ["1"],                                               7),
    ("defect_type",         ["Bug", "bug"],                                      8),
]
for _fname, _fvals, _sort in _ctfc_filters:
    _id = str(_uuid.uuid4())
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
            array({KPIS_SQL}),
            'null', 'seed-data@demo.io', CURRENT_TIMESTAMP(),
            'seed-data@demo.io', CURRENT_TIMESTAMP(),
            'user', true, {_sort}
        )
    """)
    print(f"  inserted jira/{_fname} for all CTFC KPIs")

# ── 3. Verify view now has the correct row ────────────────────────────────────
row = spark.sql(f"""
    SELECT level_3, kpi_uuids, project_name, board_ids, issue_status
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = 'demo-acme-corp'
      AND kpi_uuids = 'f60d8a58-7c8d-4dd6-9b54-6c07715ae5ec'
    LIMIT 1
""").collect()
print(f"\nview row for f60d8a58 present: {len(row) > 0}")
if row:
    print(json.dumps(row[0].asDict(), default=str, indent=2))
