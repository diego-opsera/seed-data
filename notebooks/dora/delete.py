CATALOG = "playground_prod"
SCHEMA  = "consumption_layer"

# All sdm_* tables scoped by level = 'demo-acme-corp'
_TABLES = [
    "sdm",
    "sdm_daily_snapshot",
    "sdm_weekly_snapshot",
    "sdm_df",
    "sdm_df_wkly",
    "sdm_ltfc",
    "sdm_ltfc_wkly",
    "sdm_cfr",
    "sdm_cfr_wkly",
    "sdm_mttr",
    "sdm_mttr_wkly",
    "sdm_ctfc",
    "sdm_ctfc_wkly",
]

for tbl in _TABLES:
    try:
        spark.sql(
            f"DELETE FROM {CATALOG}.{SCHEMA}.{tbl} WHERE level = 'demo-acme-corp'"
        )
        n = spark.sql(
            f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{tbl} WHERE level = 'demo-acme-corp'"
        ).collect()[0][0]
        print(f"{tbl}: deleted  ({n} rows remaining — should be 0)")
    except Exception as e:
        print(f"{tbl}: ERROR — {e}")
