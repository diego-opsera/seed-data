"""
diag_release_3.py — Check current state of release_management_detail
after ALTER TABLE + generator fix.

Answers:
  1. What level_1 values exist in the table right now?
  2. What does filter_groups_unity say level_1 is for Acme and Meridian?
  3. Does the API-style WHERE level_1 IN (...) query return data?
"""

CATALOG = "playground_prod"

# ── 1. All distinct level_1 values currently in the table ────────────────────
print("\n=== 1. Distinct level_1 values in release_management_detail ===")
spark.sql(f"""
    SELECT level_1, COUNT(*) AS rows
    FROM {CATALOG}.consumption_layer.release_management_detail
    GROUP BY level_1
    ORDER BY rows DESC
""").show(20, False)

# ── 2. filter_groups_unity level_1 for Acme and Meridian ────────────────────
print("\n=== 2. filter_groups_unity level_1 for Acme and Meridian ===")
spark.sql(f"""
    SELECT level_1, level_3, filter_group_id, createdBy
    FROM {CATALOG}.master_data.filter_groups_unity
    WHERE createdBy IN ('seed-data-meridian@demo.io')
       OR level_1 = 'Acme Corp'
       OR level_3 = 'demo-acme-corp'
""").show(10, False)

# ── 3. Simulate the API queries ───────────────────────────────────────────────
print("\n=== 3a. WHERE level_1 IN ('Acme Corp') ===")
spark.sql(f"""
    SELECT fix_version, issue_project, level_1, release_status, release_date
    FROM {CATALOG}.consumption_layer.release_management_detail
    WHERE level_1 IN ('Acme Corp')
    ORDER BY release_date DESC
""").show(10, False)

print("\n=== 3b. WHERE level_1 IN ('Meridian Analytics') ===")
spark.sql(f"""
    SELECT fix_version, issue_project, level_1, release_status, release_date
    FROM {CATALOG}.consumption_layer.release_management_detail
    WHERE level_1 IN ('Meridian Analytics')
    ORDER BY release_date DESC
""").show(10, False)

# ── 4. All rows for Acme and Meridian (by fix_version prefix) ────────────────
print("\n=== 4a. All Acme rows (fix_version LIKE 'demo-%') ===")
spark.sql(f"""
    SELECT fix_version, issue_project, level_1, level_value, release_status
    FROM {CATALOG}.consumption_layer.release_management_detail
    WHERE fix_version LIKE 'demo-%'
    ORDER BY fix_version
""").show(10, False)

print("\n=== 4b. All Meridian rows (fix_version LIKE 'meridian-%') ===")
spark.sql(f"""
    SELECT fix_version, issue_project, level_1, level_value, release_status
    FROM {CATALOG}.consumption_layer.release_management_detail
    WHERE fix_version LIKE 'meridian-%'
    ORDER BY fix_version
""").show(10, False)
