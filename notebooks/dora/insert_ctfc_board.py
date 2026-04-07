# Patch: insert jira_boards row for board_id=1 (required for CTFC chart join).
# Run if insert_ctfc_filter.py was already run without the board insert.
# Run via exec(open("/tmp/seed-data/notebooks/dora/insert_ctfc_board.py").read())

CATALOG = "playground_prod"

spark.sql(f"""
    INSERT INTO {CATALOG}.source_to_stage.raw_jira_boards_ci
        (message, board_id, board_name, board_type, org_name)
    VALUES (
        '{{"id": 1, "name": "ACME Board", "type": "scrum"}}',
        1, 'ACME Board', 'scrum', 'demo-acme-direct'
    )
""")

n = spark.sql(f"""
    SELECT COUNT(*) FROM {CATALOG}.source_to_stage.raw_jira_boards_ci WHERE board_id = 1
""").collect()[0][0]
print(f"raw_jira_boards_ci board_id=1: {n} row(s) present")
