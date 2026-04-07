# Patch: mirror seed-data rows from mt_itsm_issues_current into mt_itsm_issues_hist.
# Run ONCE in environments where itsm_issues generator ran before the hist fix.
# Safe to run from a clean environment after a data wipe.
# Run via exec(open("/tmp/seed-data/notebooks/dora/insert_ctfc_hist.py").read())

CATALOG = "playground_prod"

# ── Mirror current → hist ─────────────────────────────────────────────────────
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
