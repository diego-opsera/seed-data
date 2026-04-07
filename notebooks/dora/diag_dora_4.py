# Diagnostic round 4: understand filter_groups_unity in detail + v_filter_group_values_kpi_flattened_unity
# Goal: learn what a complete row looks like so we can insert a demo-acme-corp entry
# Run via exec(notebook.read()) in the Databricks notebook

CATALOG = "playground_prod"

# ── 1. filter_groups_unity: full schema ─────────────────────────────────────
print("=== DESCRIBE master_data.filter_groups_unity ===")
spark.sql(f"DESCRIBE {CATALOG}.master_data.filter_groups_unity").show(60, False)

# ── 2. filter_groups_unity: row count ───────────────────────────────────────
n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.master_data.filter_groups_unity").collect()[0]["n"]
print(f"\nTotal rows: {n:,}")

# ── 3. filter_groups_unity: 10 full rows (all columns) ──────────────────────
print("\n=== filter_groups_unity: 10 full rows ===")
spark.sql(f"SELECT * FROM {CATALOG}.master_data.filter_groups_unity LIMIT 10").show(10, False)

# ── 4. filter_groups_unity: distinct level_1 values (top of org tree) ────────
print("\n=== filter_groups_unity: distinct level_1 values ===")
spark.sql(f"""
    SELECT level_1, COUNT(*) AS n
    FROM {CATALOG}.master_data.filter_groups_unity
    GROUP BY level_1 ORDER BY n DESC LIMIT 20
""").show(20, False)

# ── 5. filter_groups_unity: drill into one org — show level_2/3/4/5 breakdown
print("\n=== filter_groups_unity: level hierarchy for first org ===")
spark.sql(f"""
    SELECT level_1, level_2, level_3, level_4, level_5, id
    FROM {CATALOG}.master_data.filter_groups_unity
    ORDER BY level_1, level_2, level_3, level_4, level_5
    LIMIT 20
""").show(20, False)

# ── 6. filter_groups_unity: look for any demo / acme / opsera rows ───────────
print("\n=== filter_groups_unity: rows containing 'demo', 'acme', or 'opsera' ===")
spark.sql(f"""
    SELECT *
    FROM {CATALOG}.master_data.filter_groups_unity
    WHERE lower(level_1) LIKE '%demo%' OR lower(level_1) LIKE '%acme%' OR lower(level_1) LIKE '%opsera%'
       OR lower(level_2) LIKE '%demo%' OR lower(level_2) LIKE '%acme%' OR lower(level_2) LIKE '%opsera%'
       OR lower(level_3) LIKE '%demo%' OR lower(level_3) LIKE '%acme%' OR lower(level_3) LIKE '%opsera%'
       OR lower(level_4) LIKE '%demo%' OR lower(level_4) LIKE '%acme%' OR lower(level_4) LIKE '%opsera%'
       OR lower(level_5) LIKE '%demo%' OR lower(level_5) LIKE '%acme%' OR lower(level_5) LIKE '%opsera%'
    LIMIT 20
""").show(20, False)

# ── 7. filter_groups_unity: SHOW CREATE TABLE (catch all hidden columns/types)
print("\n=== SHOW CREATE TABLE master_data.filter_groups_unity ===")
spark.sql(f"SHOW CREATE TABLE {CATALOG}.master_data.filter_groups_unity").show(1, False)

# ── 8. v_filter_group_values_kpi_flattened_unity: schema ────────────────────
print("\n=== DESCRIBE master_data.v_filter_group_values_kpi_flattened_unity ===")
spark.sql(f"DESCRIBE {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity").show(60, False)

# ── 9. v_filter_group_values_kpi_flattened_unity: row count ─────────────────
n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity").collect()[0]["n"]
print(f"\nTotal rows: {n:,}")

# ── 10. v_filter_group_values_kpi_flattened_unity: 5 full rows ───────────────
print("\n=== v_filter_group_values_kpi_flattened_unity: SELECT * LIMIT 5 ===")
spark.sql(f"SELECT * FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity LIMIT 5").show(5, False)

# ── 11. v_filter_group_values_kpi_flattened_unity: distinct level_1 values ──
print("\n=== v_filter_group_values_kpi_flattened_unity: distinct level_1 values ===")
spark.sql(f"""
    SELECT level_1, COUNT(*) AS n
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    GROUP BY level_1 ORDER BY n DESC LIMIT 20
""").show(20, False)

# ── 12. v_filter_group_values_kpi_flattened_unity: look for demo/acme rows ──
print("\n=== v_filter_group_values_kpi_flattened_unity: rows containing 'demo' or 'acme' ===")
spark.sql(f"""
    SELECT *
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE lower(level_1) LIKE '%demo%' OR lower(level_1) LIKE '%acme%'
       OR lower(level_2) LIKE '%demo%' OR lower(level_2) LIKE '%acme%'
       OR lower(level_3) LIKE '%demo%' OR lower(level_3) LIKE '%acme%'
       OR lower(level_4) LIKE '%demo%' OR lower(level_4) LIKE '%acme%'
       OR lower(level_5) LIKE '%demo%' OR lower(level_5) LIKE '%acme%'
    LIMIT 10
""").show(10, False)

# ── 13. v_filter_group_values_kpi_flattened_unity: DDL ──────────────────────
print("\n=== SHOW CREATE TABLE master_data.v_filter_group_values_kpi_flattened_unity ===")
spark.sql(f"SHOW CREATE TABLE {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity").show(1, False)
