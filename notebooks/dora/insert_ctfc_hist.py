# Patch: mirror seed-data rows from mt_itsm_issues_current into mt_itsm_issues_hist.
# Also fixes the duplicate board_id=1 in raw_jira_boards_ci.
# Run ONCE in environments where itsm_issues generator ran before the hist fix.
# Run via exec(open("/tmp/seed-data/notebooks/dora/insert_ctfc_hist.py").read())

CATALOG = "playground_prod"

# ── 1. Fix duplicate board_id=1 (delete all, re-insert once) ─────────────────
spark.sql(f"""
    DELETE FROM {CATALOG}.source_to_stage.raw_jira_boards_ci WHERE board_id = 1
""")
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
print(f"raw_jira_boards_ci board_id=1: {n} row(s) (should be 1)")

# ── 2. Mirror current → hist ──────────────────────────────────────────────────
before = spark.sql(f"""
    SELECT COUNT(*) FROM {CATALOG}.transform_stage.mt_itsm_issues_hist
    WHERE record_inserted_by = 'seed-data'
""").collect()[0][0]

spark.sql(f"""
    INSERT INTO {CATALOG}.transform_stage.mt_itsm_issues_hist
    SELECT * FROM {CATALOG}.transform_stage.mt_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
""")

after = spark.sql(f"""
    SELECT COUNT(*) FROM {CATALOG}.transform_stage.mt_itsm_issues_hist
    WHERE record_inserted_by = 'seed-data'
""").collect()[0][0]
print(f"mt_itsm_issues_hist: {before} → {after} seed-data rows")
