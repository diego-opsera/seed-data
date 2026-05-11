# AI Code Comparison dashboard — diagnostic script
#
# Run when the dashboard shows "No data available" and you want to pinpoint
# whether the issue is (a) data not inserted, (b) filter rows not inserted,
# (c) the v_filter_group_values_kpi_flattened_unity view not surfacing our
# rows, or (d) a KPI-UUID format mismatch.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/ai_compare/debug_dashboard.py").read())

CATALOG  = "playground_prod"
TEST_ORG = "demo-acme-direct"

print()
print("=" * 78)
print("1. Row counts in new (ai_compare-owned) tables")
print("=" * 78)
for tbl, scope in [
    ("consumption_layer.ai_assistant_license_info",             f"access_level_name = '{TEST_ORG}'"),
    ("consumption_layer.ai_assistant_user_engagement",          f"level_name = '{TEST_ORG}'"),
    ("consumption_layer.ai_assistant_programming_language_agg", f"level_type_name = '{TEST_ORG}'"),
    ("consumption_layer.ai_assistant_language_model_metrics",   f"level_name = '{TEST_ORG}'"),
    ("consumption_layer.commits_prs",                           f"org_name = '{TEST_ORG}'"),
]:
    try:
        n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{tbl} WHERE {scope}").collect()[0][0]
        print(f"  {tbl}: {n} rows  (scope: {scope})")
    except Exception as e:
        print(f"  {tbl}: ERROR — {str(e).splitlines()[0][:160]}")

print()
print("=" * 78)
print("2. Per-tool counts in shared tables (must contain all 3 tools)")
print("=" * 78)
print("-- ai_assistant_acceptance_info --")
spark.sql(f"""
  SELECT ai_assistant_tool_name, COUNT(*) AS rows,
         MIN(ai_assistant_usage_date) AS min_date,
         MAX(ai_assistant_usage_date) AS max_date
  FROM {CATALOG}.consumption_layer.ai_assistant_acceptance_info
  WHERE level_name = '{TEST_ORG}'
  GROUP BY 1 ORDER BY 1
""").show(truncate=False)

print("-- ai_code_assistant_usage_user_level --")
spark.sql(f"""
  SELECT ai_tool_name, COUNT(*) AS rows,
         MIN(last_activity_date) AS min_date,
         MAX(last_activity_date) AS max_date
  FROM {CATALOG}.consumption_layer.ai_code_assistant_usage_user_level
  WHERE org_name = '{TEST_ORG}'
  GROUP BY 1 ORDER BY 1
""").show(truncate=False)

print()
print("=" * 78)
print("3. Filter rows seeded by this batch")
print("=" * 78)
spark.sql(f"""
  SELECT tool_type, filter_name, filter_values, size(kpi_uuids) AS n_kpis,
         filter_group_id, created_by
  FROM {CATALOG}.master_data.filter_values_unity
  WHERE created_by = 'seed-data-ai-compare@demo.io'
  ORDER BY tool_type
""").show(truncate=False)

print()
print("=" * 78)
print("4. Acme filter_group_id lookup")
print("=" * 78)
fg_rows = spark.sql(f"""
  SELECT filter_group_id, level_1, level_3, createdBy, createdAt
  FROM {CATALOG}.master_data.filter_groups_unity
  WHERE createdBy = 'seed-data@demo.io' AND level_3 = 'demo-acme-corp'
  ORDER BY createdAt DESC
""").collect()

if not fg_rows:
    print("  NO ACME FILTER_GROUP FOUND — run notebooks/dora/insert.py first")
    fg_id = None
else:
    for r in fg_rows:
        print(f"  filter_group_id={r['filter_group_id']}  level_1={r['level_1']!r}  createdAt={r['createdAt']}")
    fg_id = fg_rows[0]["filter_group_id"]
    if len(fg_rows) > 1:
        print(f"  WARN: {len(fg_rows)} filter_groups found — ai_compare/insert.py picks the first (most recent)")

print()
print("=" * 78)
print("5. Does v_filter_group_values_kpi_flattened_unity surface our tool_type rows?")
print("=" * 78)
print("   (The view pivots filter_name/filter_values into typed columns —")
print("    tool_type, org_name, team_names, project_url, etc. — so we query")
print("    by tool_type directly, not by filter_name.)")
print()
print("-- Our tool_type rows as the view sees them --")
if fg_id:
    spark.sql(f"""
      SELECT tool_type, org_name, team_names, kpi_uuids
      FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
      WHERE filter_group_id = '{fg_id}'
        AND tool_type IN ('github copilot', 'cursor', 'claude code')
    """).show(50, truncate=False)

print()
print("-- DISTINCT kpi_uuid values present for the Acme filter_group --")
print("   (the view pre-flattens kpi_uuids into a STRING — one row per KPI)")
if fg_id:
    spark.sql(f"""
      SELECT DISTINCT kpi_uuids AS kpi_uuid
      FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
      WHERE filter_group_id = '{fg_id}'
      ORDER BY 1
    """).show(50, truncate=False)

print()
print("=" * 78)
print("6. Simulate the dashboard's distinct_tool query")
print("=" * 78)
print("   (this is what populates the AI Tool dropdown — should return 3 rows)")
print()
print("-- 6a. Minimal: by kpi_uuid only --")
spark.sql(f"""
  WITH filters AS (
    SELECT coalesce(tool_type,'x') AS tool_type
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    LATERAL VIEW explode_outer(org_name) AS org_name_col
    LATERAL VIEW explode_outer(team_names) AS team_col
    WHERE kpi_uuids = 'ai_code_comparison_distinct_tool'
  )
  SELECT DISTINCT tool_type AS ai_assistant_tool_name FROM filters
""").show(truncate=False)

print("-- 6b. With filter_group_id scope (what the dashboard likely does) --")
if fg_id:
    spark.sql(f"""
      WITH filters AS (
        SELECT coalesce(tool_type,'x') AS tool_type
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        LATERAL VIEW explode_outer(org_name) AS org_name_col
        LATERAL VIEW explode_outer(team_names) AS team_col
        WHERE filter_group_id = '{fg_id}'
          AND kpi_uuids = 'ai_code_comparison_distinct_tool'
      )
      SELECT DISTINCT tool_type AS ai_assistant_tool_name FROM filters
    """).show(truncate=False)

print("-- 6c. With level scope (Project=Acme Corp, Product=demo-acme-corp) --")
spark.sql(f"""
  WITH filters AS (
    SELECT coalesce(tool_type,'x') AS tool_type
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    LATERAL VIEW explode_outer(org_name) AS org_name_col
    LATERAL VIEW explode_outer(team_names) AS team_col
    WHERE level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
      AND kpi_uuids = 'ai_code_comparison_distinct_tool'
  )
  SELECT DISTINCT tool_type AS ai_assistant_tool_name FROM filters
""").show(truncate=False)

print()
print("=" * 78)
print("7. View schema sanity (column names + types — useful if joins fail)")
print("=" * 78)
spark.sql(f"DESCRIBE {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity").show(50, truncate=False)

print()
print("=" * 78)
print("8. Compare to a known-working DORA filter row in the same filter_group")
print("=" * 78)
print("   (DORA is rendering correctly today, so its filter rows are a working")
print("    reference for what dashboard-visible filter rows look like.)")
if fg_id:
    spark.sql(f"""
      SELECT tool_type, org_name, team_names, project_url, deployment_stages,
             kpi_uuids
      FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
      WHERE filter_group_id = '{fg_id}'
        AND tool_type IN ('github', 'jira')
      LIMIT 20
    """).show(truncate=False)

print()
print("=" * 78)
print("Diagnostic complete.")
print("=" * 78)
print("""
Interpretation guide:
  - Sections 1-4 all populated     → data + filter rows are landing fine.
  - Section 5 first table empty    → view doesn't propagate our rows; check
                                     whether tool_type column survives the view's
                                     pivot logic.
  - Section 6a returns 3 rows      → unscoped distinct_tool query sees our tools.
  - Section 6b returns 3 rows      → scoped by filter_group_id, dashboard match.
  - Section 6c returns 3 rows      → scoped by level_1/level_3, dashboard match.
  - Section 6a yes, 6b/6c no       → view doesn't carry filter_group_id or level
                                     columns; section 7 schema tells us which.
  - All of 6a/6b/6c return 0 rows  → kpi_uuids stored as string isn't matching.
                                     Check that the view's kpi_uuids really
                                     equals the literal we passed (vs trimmed,
                                     cased, etc.).
  - Section 8 has dora rows but    → DORA filter rows have org_name / project_url
    different columns populated      populated where ours have NULLs — may indicate
                                     a column the view requires for visibility.
""")
