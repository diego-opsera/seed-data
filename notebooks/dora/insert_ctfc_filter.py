# One-time patch: add CTFC Jira filter rows to existing demo-acme-corp filter group,
# and update board_info on existing seed-data Jira issues.
# Run ONCE in environments where insert_filter_group.py already ran (before the CTFC fix).
# Run via exec(open("/tmp/seed-data/notebooks/dora/insert_ctfc_filter.py").read())

import uuid, json

CATALOG = "playground_prod"
CTFC_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"

# Hardcoded filter_group_id created by insert_filter_group.py for demo-acme-corp
FILTER_GROUP_ID = "d277535f-a8cb-4429-965d-a9de685b4045"

# ── 0. Insert jira_boards row for board_id=1 (required for CTFC chart join) ──
spark.sql(f"""
    INSERT INTO {CATALOG}.source_to_stage.raw_jira_boards_ci
        (message, board_id, board_name, board_type, org_name)
    VALUES (
        '{{"id": 1, "name": "ACME Board", "type": "scrum"}}',
        1, 'ACME Board', 'scrum', 'demo-acme-direct'
    )
""")
print("jira_boards: inserted board_id=1 (ACME Board)")

# ── 1. Insert 5 Jira CTFC filter rows ────────────────────────────────────────
_ctfc_filters = [
    ("project_name",        ["ACME"],                                           4),
    ("issue_status",        ["Done", "done", "Completed"],                      5),
    ("include_issue_types", ["Story", "story", "Bug", "bug", "Task", "task"],   6),
    ("board_ids",           ["1"],                                               7),
    ("defect_type",         ["Bug", "bug"],                                      8),
]
inserted = []
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
    inserted.append({"filter_name": _fname, "id": _id})
    print(f"  inserted jira/{_fname}")

# ── 2. Update board_info on existing seed-data issues ────────────────────────
spark.sql(f"""
    UPDATE {CATALOG}.transform_stage.mt_itsm_issues_current
    SET board_info = ARRAY(NAMED_STRUCT(
        'board_id', CAST(1 AS BIGINT),
        'board_name', 'ACME Board',
        'board_type', 'scrum'
    ))
    WHERE record_inserted_by = 'seed-data'
      AND board_info = ARRAY()
""")
n = spark.sql(f"""
    SELECT COUNT(*) FROM {CATALOG}.transform_stage.mt_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
      AND SIZE(board_info) > 0
""").collect()[0][0]
print(f"\nboard_info updated on {n} seed-data issues")

# ── 3. Verify view has the new row ────────────────────────────────────────────
rows = spark.sql(f"""
    SELECT level_3, kpi_uuids, project_name, board_ids, issue_status
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = 'demo-acme-corp' AND kpi_uuids = '{CTFC_KPI}'
    LIMIT 1
""").collect()
print(f"\nview row present: {len(rows) > 0}")
if rows:
    r = rows[0].asDict()
    print(json.dumps(r, default=str, indent=2))
