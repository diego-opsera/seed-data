"""
diag_release_2.py — Confirm the exact failure mode and test the fix approach.

Root-cause hypothesis:
  The API builds: WHERE level_1 IN ('Meridian Analytics')
  But the table has a column called `level_name` (not `level_1`).
  Querying a non-existent column causes an error → dashboard shows "no data".

This script:
  1. Confirms the WHERE level_1 IN (...) query fails (column not found)
  2. Confirms the compound filter approach works
  3. Checks if level_1-level_5 columns exist (via DESCRIBE, already done — they don't)
  4. Lists any views in consumption_layer
  5. Shows Acme's filter_groups structure for comparison
"""

CATALOG = "playground_prod"

# ── 1. Try the EXACT query the API generates — expect a column error ──────────
print("\n=== 1. API-style WHERE level_1 IN ('Meridian Analytics') — should fail ===")
try:
    result = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {CATALOG}.consumption_layer.release_management_detail
        WHERE level_1 IN ('Meridian Analytics')
    """).collect()[0][0]
    print(f"  Returned {result} rows — unexpected, level_1 column apparently exists!")
except Exception as e:
    print(f"  FAILED (expected): {str(e)[:200]}")

# ── 2. Confirm the compound filter (level_name + level_value) works ───────────
print("\n=== 2. Compound filter WHERE level_name='level_1' AND level_value IN (...) ===")
try:
    result = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {CATALOG}.consumption_layer.release_management_detail
        WHERE level_name = 'level_1'
          AND level_value IN ('Meridian Analytics')
    """).collect()[0][0]
    print(f"  Returned {result} rows (should be 5)")
except Exception as e:
    print(f"  error: {e}")

# ── 3. Views in consumption_layer (proper catalog-qualified syntax) ────────────
print("\n=== 3. Views in playground_prod.consumption_layer ===")
try:
    spark.sql("USE CATALOG playground_prod")
    spark.sql("SHOW VIEWS IN consumption_layer").show(20, False)
except Exception as e:
    print(f"  error: {e}")

# ── 4. Acme's filter_groups_unity structure ───────────────────────────────────
# Understand what level Acme uses — does it have level_3='demo-acme-corp'?
print("\n=== 4. Acme filter_groups_unity rows (createdBy or level values with 'acme'/'demo') ===")
try:
    spark.sql(f"""
        SELECT id, level_1, level_2, level_3, level_4, level_5,
               filter_group_id, createdBy, active
        FROM {CATALOG}.master_data.filter_groups_unity
        WHERE level_1 LIKE '%acme%' OR level_2 LIKE '%acme%'
           OR level_3 LIKE '%acme%' OR level_1 LIKE '%demo%'
           OR level_3 LIKE '%demo%'
        LIMIT 10
    """).show(10, False)
except Exception as e:
    print(f"  error: {e}")

# ── 5. Sample row from Acme release data to see its level_name/level_value ────
print("\n=== 5. Acme release rows — actual level_name and level_value stored ===")
try:
    spark.sql(f"""
        SELECT level_name, level_value, fix_version, issue_project, release_status
        FROM {CATALOG}.consumption_layer.release_management_detail
        WHERE fix_version LIKE 'demo-%'
        LIMIT 5
    """).show(5, False)
except Exception as e:
    print(f"  error: {e}")

# ── 6. Can we ALTER TABLE to add level_1..level_5 columns? ───────────────────
# Test whether the table accepts new columns by checking existing columns.
# (Don't run the ALTER yet — just confirm the DESCRIBE is current)
print("\n=== 6. Confirm no level_1 column exists (re-check DESCRIBE) ===")
try:
    spark.sql(f"""
        SELECT col_name, data_type
        FROM (DESCRIBE {CATALOG}.consumption_layer.release_management_detail)
        WHERE col_name IN ('level_1','level_2','level_3','level_4','level_5','level_name','level_value')
    """).show(10, False)
except Exception as e:
    print(f"  error: {e}")

# ── 7. Test the proposed fix: ALTER + UPDATE ──────────────────────────────────
# WARNING: This modifies the table schema. Only run section 7 if sections 1-6
# confirm the hypothesis (level_1 column is missing and query fails).
#
# Uncomment and run separately after confirming above:
#
# spark.sql(f"""
#     ALTER TABLE {CATALOG}.consumption_layer.release_management_detail
#     ADD COLUMNS (level_1 STRING, level_2 STRING, level_3 STRING,
#                  level_4 STRING, level_5 STRING)
# """)
# print("ALTER TABLE done")
#
# spark.sql(f"""
#     UPDATE {CATALOG}.consumption_layer.release_management_detail
#     SET level_1 = level_value
#     WHERE level_name = 'level_1'
#       AND fix_version LIKE 'meridian-%'
# """)
# print("UPDATE done — level_1 populated for Meridian rows")
#
# spark.sql(f"""
#     SELECT level_name, level_value, level_1, fix_version
#     FROM {CATALOG}.consumption_layer.release_management_detail
#     WHERE fix_version LIKE 'meridian-%'
# """).show(5, False)
