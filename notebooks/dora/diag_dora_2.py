# Diagnostic round 2: schemas and sample data for SDM / DORA tables
# Run each cell via exec(notebook.read()) in the Databricks notebook

CATALOG = "playground_prod"

# ── 1. SDM core tables ───────────────────────────────────────────────────────
for tbl in ["sdm", "sdm_daily_snapshot", "sdm_weekly_snapshot",
            "sdm_df", "sdm_df_wkly",
            "sdm_ltfc", "sdm_ltfc_wkly",
            "sdm_cfr", "sdm_cfr_wkly",
            "sdm_mttr", "sdm_mttr_wkly",
            "sdm_ctfc", "sdm_ctfc_wkly"]:
    print(f"\n=== DESCRIBE consumption_layer.{tbl} ===")
    try:
        spark.sql(f"DESCRIBE {CATALOG}.consumption_layer.{tbl}").show(60, False)
    except Exception as e:
        print(f"  error: {e}")

# ── 2. SDM core tables: row counts and scoping columns ──────────────────────
for tbl in ["sdm", "sdm_daily_snapshot", "sdm_df", "sdm_ltfc", "sdm_cfr", "sdm_mttr"]:
    print(f"\n=== consumption_layer.{tbl}: row count ===")
    try:
        n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.consumption_layer.{tbl}").collect()[0]["n"]
        print(f"  {n:,} rows")
    except Exception as e:
        print(f"  error: {e}")

# ── 3. SDM core tables: sample rows (look for org/customer scoping columns) ──
for tbl in ["sdm", "sdm_daily_snapshot", "sdm_df", "sdm_ltfc", "sdm_cfr", "sdm_mttr"]:
    print(f"\n=== consumption_layer.{tbl}: sample 3 rows ===")
    try:
        spark.sql(f"SELECT * FROM {CATALOG}.consumption_layer.{tbl} LIMIT 3").show(3, False)
    except Exception as e:
        print(f"  error: {e}")

# ── 4. pipeline_activities ───────────────────────────────────────────────────
print("\n=== DESCRIBE base_datasets.pipeline_activities ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.base_datasets.pipeline_activities").show(80, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== base_datasets.pipeline_activities: row count ===")
try:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.base_datasets.pipeline_activities").collect()[0]["n"]
    print(f"  {n:,} rows")
except Exception as e:
    print(f"  error: {e}")

print("\n=== base_datasets.pipeline_activities: sample 3 rows ===")
try:
    spark.sql(f"SELECT * FROM {CATALOG}.base_datasets.pipeline_activities LIMIT 3").show(3, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== base_datasets.pipeline_activities: distinct org/customer scoping values ===")
try:
    # try common scoping column names
    for col in ["org_name", "customer_id", "organization_id", "org", "level_value", "customer"]:
        try:
            spark.sql(f"""
                SELECT '{col}' AS col_name, {col}, COUNT(*) AS n
                FROM {CATALOG}.base_datasets.pipeline_activities
                GROUP BY {col} ORDER BY n DESC LIMIT 10
            """).show(10, False)
            break
        except Exception:
            continue
except Exception as e:
    print(f"  error: {e}")

# ── 5. pipeline_deployment_commits ──────────────────────────────────────────
print("\n=== DESCRIBE base_datasets.pipeline_deployment_commits ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.base_datasets.pipeline_deployment_commits").show(60, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== base_datasets.pipeline_deployment_commits: row count ===")
try:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.base_datasets.pipeline_deployment_commits").collect()[0]["n"]
    print(f"  {n:,} rows")
except Exception as e:
    print(f"  error: {e}")

print("\n=== base_datasets.pipeline_deployment_commits: sample 3 rows ===")
try:
    spark.sql(f"SELECT * FROM {CATALOG}.base_datasets.pipeline_deployment_commits LIMIT 3").show(3, False)
except Exception as e:
    print(f"  error: {e}")

# ── 6. cfr_mttr_metric_data ──────────────────────────────────────────────────
print("\n=== DESCRIBE base_datasets.cfr_mttr_metric_data ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.base_datasets.cfr_mttr_metric_data").show(60, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== base_datasets.cfr_mttr_metric_data: row count ===")
try:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.base_datasets.cfr_mttr_metric_data").collect()[0]["n"]
    print(f"  {n:,} rows")
except Exception as e:
    print(f"  error: {e}")

print("\n=== base_datasets.cfr_mttr_metric_data: sample 3 rows ===")
try:
    spark.sql(f"SELECT * FROM {CATALOG}.base_datasets.cfr_mttr_metric_data LIMIT 3").show(3, False)
except Exception as e:
    print(f"  error: {e}")

# ── 7. Views: check what tables they query (get the view DDL) ────────────────
for view in ["deployment_frequency_metric_view", "lead_time_for_changes_metric_view",
             "df_standard_view", "ltfc_standard_view"]:
    print(f"\n=== DDL for consumption_layer.{view} ===")
    try:
        spark.sql(f"SHOW CREATE TABLE {CATALOG}.consumption_layer.{view}").show(1, False)
    except Exception as e:
        print(f"  error: {e}")

# ── 8. Check release_management_detail_v2 (may be the DORA-linked version) ──
print("\n=== DESCRIBE consumption_layer.release_management_detail_v2 ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.consumption_layer.release_management_detail_v2").show(50, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== release_management_detail_v2: row count and scoping ===")
try:
    spark.sql(f"""
        SELECT level_name, level_value, COUNT(*) AS n
        FROM {CATALOG}.consumption_layer.release_management_detail_v2
        GROUP BY level_name, level_value ORDER BY n DESC LIMIT 20
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")
