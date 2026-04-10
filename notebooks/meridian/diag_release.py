"""
Diagnostics for the Meridian release management dashboard showing "No Data".

Run each section independently in a Databricks notebook cell.
All queries are read-only — nothing is modified.
"""

CATALOG = "playground_prod"

# ── 1. Are our rows actually in the table? ────────────────────────────────────
print("\n=== 1. Meridian row count in release_management_detail ===")
try:
    spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {CATALOG}.consumption_layer.release_management_detail
        WHERE fix_version LIKE 'meridian-%'
    """).show()
except Exception as e:
    print(f"  error: {e}")

# ── 2. Actual level_name / level_value written for our rows ───────────────────
print("\n=== 2. level_name + level_value for Meridian fix_versions ===")
try:
    spark.sql(f"""
        SELECT level_name, level_value, fix_version, issue_project,
               release_date, release_status
        FROM {CATALOG}.consumption_layer.release_management_detail
        WHERE fix_version LIKE 'meridian-%'
        ORDER BY release_date
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")

# ── 3. All distinct level_name / level_value combos in the table ──────────────
# Helps confirm what values the dashboard is filtering against for Acme.
print("\n=== 3. All distinct (level_name, level_value) in release_management_detail ===")
try:
    spark.sql(f"""
        SELECT level_name, level_value, COUNT(*) AS rows
        FROM {CATALOG}.consumption_layer.release_management_detail
        GROUP BY level_name, level_value
        ORDER BY rows DESC
    """).show(30, False)
except Exception as e:
    print(f"  error: {e}")

# ── 4. filter_groups_unity — Meridian row ─────────────────────────────────────
# Verify the level_1 / level_3 values and what filter_group_id was assigned.
print("\n=== 4. filter_groups_unity row for Meridian ===")
try:
    spark.sql(f"""
        SELECT id, level_1, level_2, level_3, level_4, level_5,
               filter_group_id, createdBy, active
        FROM {CATALOG}.master_data.filter_groups_unity
        WHERE createdBy = 'seed-data-meridian@demo.io'
    """).show(5, False)
except Exception as e:
    print(f"  error: {e}")

# ── 5. filter_values_unity — is there a release management KPI entry? ─────────
# The release dashboard may require a filter_values_unity entry for its KPI UUID.
# Check whether Acme has one and whether Meridian does too.
print("\n=== 5a. filter_values_unity for Meridian (all entries) ===")
try:
    spark.sql(f"""
        SELECT filter_group_id, tool_type, filter_name,
               filter_values, kpi_uuids, sort_number
        FROM {CATALOG}.master_data.filter_values_unity
        WHERE created_by = 'seed-data-meridian@demo.io'
        ORDER BY sort_number
    """).show(30, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== 5b. KPI UUIDs present in filter_values_unity that look like release mgmt ===")
try:
    spark.sql(f"""
        SELECT DISTINCT kpi_uuid, tool_type, filter_name, filter_values
        FROM {CATALOG}.master_data.filter_values_unity
        LATERAL VIEW EXPLODE(kpi_uuids) AS kpi_uuid
        WHERE kpi_uuid LIKE '%release%'
           OR filter_name LIKE '%release%'
           OR filter_name LIKE '%project%'
        LIMIT 20
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")

# ── 6. v_filter_group_values_kpi_flattened_unity — Meridian ──────────────────
# This view is what the API actually joins; if the view excludes Meridian rows
# the dashboard won't see them even if filter_groups_unity + filter_values_unity
# are correct.
print("\n=== 6. v_filter_group_values_kpi_flattened_unity for Meridian ===")
try:
    spark.sql(f"""
        SELECT *
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        WHERE level_1 = 'Meridian Analytics'
           OR level_3 = 'demo-meridian'
        LIMIT 20
    """).show(20, False)
except Exception as e:
    print(f"  view not found or error: {e}")

# ── 7. What the dashboard WHERE clause would actually return ──────────────────
# The API sends: level_name='level_1' AND level_value IN ('Meridian Analytics')
# Replicate that exact filter here.
print("\n=== 7. Simulate dashboard WHERE level_name='level_1' AND level_value='Meridian Analytics' ===")
try:
    spark.sql(f"""
        SELECT fix_version, issue_project, release_date, release_status,
               level_name, level_value
        FROM {CATALOG}.consumption_layer.release_management_detail
        WHERE level_name = 'level_1'
          AND level_value IN ('Meridian Analytics')
        ORDER BY release_date
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")

# Alternative: maybe level_3='demo-meridian' is sent instead of level_1
print("\n=== 7b. Simulate WHERE level_name='level_3' AND level_value='demo-meridian' ===")
try:
    spark.sql(f"""
        SELECT fix_version, issue_project, release_date, release_status,
               level_name, level_value
        FROM {CATALOG}.consumption_layer.release_management_detail
        WHERE level_name = 'level_3'
          AND level_value IN ('demo-meridian')
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")

# ── 8. Date range sanity check ────────────────────────────────────────────────
# "Last 3" in the dashboard may mean last 90 days of release_date (DATE type).
# Today = 2026-04-10. 90 days back = 2026-01-10. Check which Meridian releases fall in.
print("\n=== 8. Meridian releases in the last 90 / 180 / 270 days ===")
try:
    spark.sql(f"""
        SELECT fix_version, release_date, release_status,
               DATEDIFF(CURRENT_DATE(), release_date) AS days_ago
        FROM {CATALOG}.consumption_layer.release_management_detail
        WHERE fix_version LIKE 'meridian-%'
        ORDER BY release_date DESC
    """).show(10, False)
except Exception as e:
    print(f"  error: {e}")

# Also check Acme's date spread for comparison
print("\n=== 8b. Acme releases in release_management_detail (comparison) ===")
try:
    spark.sql(f"""
        SELECT fix_version, level_value, release_date, release_status,
               DATEDIFF(CURRENT_DATE(), release_date) AS days_ago
        FROM {CATALOG}.consumption_layer.release_management_detail
        WHERE fix_version LIKE 'demo-%'
        ORDER BY release_date DESC
        LIMIT 10
    """).show(10, False)
except Exception as e:
    print(f"  error: {e}")

# ── 9. Is there a view that wraps release_management_detail? ─────────────────
print("\n=== 9. Views in consumption_layer that reference release_management_detail ===")
try:
    spark.sql(f"""
        SHOW VIEWS IN {CATALOG}.consumption_layer
    """).show(50, False)
except Exception as e:
    print(f"  error: {e}")

# ── 10. Full table schema (confirm column order vs. our INSERT column list) ───
print("\n=== 10. DESCRIBE release_management_detail ===")
try:
    spark.sql(f"""
        DESCRIBE {CATALOG}.consumption_layer.release_management_detail
    """).show(30, False)
except Exception as e:
    print(f"  error: {e}")
