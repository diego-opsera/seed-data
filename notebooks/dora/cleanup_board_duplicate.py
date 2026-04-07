# ONE-TIME cleanup: removes the duplicate board_id=1 row in raw_jira_boards_ci
# caused by running insert_ctfc_board.py twice in the current environment.
# Do NOT run this in a fresh environment — it will leave board_id=1 with 0 rows.
# Run via exec(open("/tmp/seed-data/notebooks/dora/cleanup_board_duplicate.py").read())

CATALOG = "playground_prod"

before = spark.sql(f"""
    SELECT COUNT(*) FROM {CATALOG}.source_to_stage.raw_jira_boards_ci WHERE board_id = 1
""").collect()[0][0]

spark.sql(f"DELETE FROM {CATALOG}.source_to_stage.raw_jira_boards_ci WHERE board_id = 1")

spark.sql(f"""
    INSERT INTO {CATALOG}.source_to_stage.raw_jira_boards_ci
        (message, board_id, board_name, board_type, org_name)
    VALUES (
        '{{"id": 1, "name": "ACME Board", "type": "scrum"}}',
        1, 'ACME Board', 'scrum', 'demo-acme-direct'
    )
""")

after = spark.sql(f"""
    SELECT COUNT(*) FROM {CATALOG}.source_to_stage.raw_jira_boards_ci WHERE board_id = 1
""").collect()[0][0]
print(f"board_id=1: {before} → {after} row(s) (should be 1)")
