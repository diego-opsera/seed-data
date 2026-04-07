CATALOG = "playground_prod"
SCHEMA  = "consumption_layer"

# ── sdm_* tables ──────────────────────────────────────────────────────────────
_SDM_TABLES = [
    "sdm", "sdm_daily_snapshot", "sdm_weekly_snapshot",
    "sdm_df", "sdm_df_wkly", "sdm_ltfc", "sdm_ltfc_wkly",
    "sdm_cfr", "sdm_cfr_wkly", "sdm_mttr", "sdm_mttr_wkly",
    "sdm_ctfc", "sdm_ctfc_wkly",
]

for tbl in _SDM_TABLES:
    try:
        spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.{tbl} WHERE level = 'demo-acme-corp'")
        n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{tbl} WHERE level = 'demo-acme-corp'").collect()[0][0]
        print(f"{tbl}: deleted  ({n} remaining — should be 0)")
    except Exception as e:
        print(f"{tbl}: ERROR — {e}")

# ── pipeline_activities ───────────────────────────────────────────────────────
try:
    spark.sql(f"""
        DELETE FROM {CATALOG}.base_datasets.pipeline_activities
        WHERE record_inserted_by = 'seed-data'
          AND project_url = 'https://github.com/demo-acme/project_001.git'
    """)
    n = spark.sql(f"""
        SELECT COUNT(*) FROM {CATALOG}.base_datasets.pipeline_activities
        WHERE record_inserted_by = 'seed-data'
    """).collect()[0][0]
    print(f"pipeline_activities: deleted  ({n} remaining — should be 0)")
except Exception as e:
    print(f"pipeline_activities: ERROR — {e}")

# ── cfr_mttr_metric_data ──────────────────────────────────────────────────────
try:
    spark.sql(f"""
        DELETE FROM {CATALOG}.base_datasets.cfr_mttr_metric_data
        WHERE record_inserted_by = 'seed-data'
          AND issue_project = 'Acme Platform'
    """)
    n = spark.sql(f"""
        SELECT COUNT(*) FROM {CATALOG}.base_datasets.cfr_mttr_metric_data
        WHERE record_inserted_by = 'seed-data'
    """).collect()[0][0]
    print(f"cfr_mttr_metric_data: deleted  ({n} remaining — should be 0)")
except Exception as e:
    print(f"cfr_mttr_metric_data: ERROR — {e}")
