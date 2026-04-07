# Diagnostic: discover DORA-related tables and schemas in playground_prod
# Run each cell via exec(notebook.read()) in the Databricks notebook

CATALOG = "playground_prod"

# ── 1. Scan all schemas for DORA / deployment / incident tables ──────────────
print("=== All tables in consumption_layer ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.consumption_layer").show(100, False)

print("\n=== All tables in transform_stage ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.transform_stage").show(100, False)

print("\n=== All tables in base_datasets ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.base_datasets").show(100, False)

print("\n=== All tables in source_to_stage ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.source_to_stage").show(100, False)

print("\n=== All schemas in catalog ===")
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").show(50, False)

# ── 2. Probe known DORA / deployment candidate table names ───────────────────
_candidates = [
    "consumption_layer.dora_metrics",
    "consumption_layer.dora_summary",
    "consumption_layer.deployment_frequency",
    "consumption_layer.deployments",
    "consumption_layer.lead_time_for_changes",
    "consumption_layer.change_failure_rate",
    "consumption_layer.mean_time_to_recovery",
    "consumption_layer.incidents",
    "transform_stage.dora_metrics",
    "transform_stage.deployments",
    "transform_stage.deployment_events",
    "transform_stage.incidents",
    "base_datasets.dora_metrics",
    "base_datasets.deployments",
    "base_datasets.incidents",
    "source_to_stage.deployments",
    "source_to_stage.incidents",
]

print("\n=== Probing candidate DORA table names ===")
for tbl in _candidates:
    try:
        n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{tbl}").collect()[0]["n"]
        print(f"  EXISTS  {tbl}  ({n:,} rows)")
    except Exception as e:
        print(f"  MISSING {tbl}")

# ── 3. Full schema of release_management_detail (as deployed) ────────────────
print("\n=== DESCRIBE release_management_detail ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.consumption_layer.release_management_detail").show(50, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== release_management_detail row count and distinct level_name values ===")
try:
    spark.sql(f"""
        SELECT level_name, level_value, COUNT(*) AS n
        FROM {CATALOG}.consumption_layer.release_management_detail
        GROUP BY level_name, level_value
        ORDER BY n DESC
        LIMIT 20
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== release_management_detail sample row (1) ===")
try:
    spark.sql(f"SELECT * FROM {CATALOG}.consumption_layer.release_management_detail LIMIT 1").show(1, False)
except Exception as e:
    print(f"  error: {e}")

# ── 4. Schema of pull_requests and commits tables ────────────────────────────
print("\n=== DESCRIBE pull_requests ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.base_datasets.pull_requests").show(50, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== pull_requests sample rows (org scoped) ===")
try:
    spark.sql(f"""
        SELECT *
        FROM {CATALOG}.base_datasets.pull_requests
        WHERE org_name LIKE '%demo%'
        LIMIT 5
    """).show(5, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== DESCRIBE commits_rest_api ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.base_datasets.commits_rest_api").show(50, False)
except Exception as e:
    print(f"  error: {e}")

# ── 5. Check if pipeline_trigger_details JSON array is queryable ─────────────
# The generator stores ltfc_seconds and pipeline_status inside JSON strings.
# Verify how the dashboard actually reads them (LATERAL VIEW EXPLODE? from_json?).
print("\n=== pipeline_trigger_details first element sample (raw JSON) ===")
try:
    spark.sql(f"""
        SELECT fix_version, issue_project,
               pipeline_trigger_details[0] AS first_pipeline
        FROM {CATALOG}.consumption_layer.release_management_detail
        LIMIT 3
    """).show(3, False)
except Exception as e:
    print(f"  error: {e}")

# ── 6. Any org/tool mapping tables that reference DORA or deployments? ────────
print("\n=== master_data tables ===")
try:
    spark.sql(f"SHOW TABLES IN {CATALOG}.master_data").show(50, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== scan information_schema for 'dora' or 'deploy' or 'incident' table names ===")
try:
    spark.sql(f"""
        SELECT table_schema, table_name
        FROM {CATALOG}.information_schema.tables
        WHERE lower(table_name) LIKE '%dora%'
           OR lower(table_name) LIKE '%deploy%'
           OR lower(table_name) LIKE '%incident%'
           OR lower(table_name) LIKE '%lead_time%'
           OR lower(table_name) LIKE '%failure_rate%'
           OR lower(table_name) LIKE '%mttr%'
        ORDER BY table_schema, table_name
    """).show(50, False)
except Exception as e:
    print(f"  error: {e}")
