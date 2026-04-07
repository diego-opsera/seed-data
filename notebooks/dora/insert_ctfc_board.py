# Patch: insert jira_boards row for board_id=1 (required for CTFC chart join).
# Run if insert_ctfc_filter.py was already run without the board insert.
# Run via exec(open("/tmp/seed-data/notebooks/dora/insert_ctfc_board.py").read())

CATALOG = "playground_prod"

spark.sql(f"""
    INSERT INTO {CATALOG}.base_datasets.jira_boards
        (board_id, board_name, board_type, project_name)
    VALUES (1, 'ACME Board', 'scrum', null)
""")

n = spark.sql(f"""
    SELECT COUNT(*) FROM {CATALOG}.base_datasets.jira_boards WHERE board_id = 1
""").collect()[0][0]
print(f"jira_boards board_id=1: {n} row(s) present")
