# Diagnostic round 3: understand the hierarchy filter mapping tables
# These back the Organization/Product/Application/Project dropdowns in the DORA UI
# Run via exec(notebook.read()) in the Databricks notebook

CATALOG = "playground_prod"

# ── 1. filter_groups_unity schema + sample ───────────────────────────────────
print("=== DESCRIBE master_data.filter_groups_unity ===")
spark.sql(f"DESCRIBE {CATALOG}.master_data.filter_groups_unity").show(40, False)

print("\n=== filter_groups_unity: row count ===")
n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.master_data.filter_groups_unity").collect()[0]["n"]
print(f"  {n:,} rows")

print("\n=== filter_groups_unity: sample 5 rows ===")
spark.sql(f"SELECT * FROM {CATALOG}.master_data.filter_groups_unity LIMIT 5").show(5, False)

print("\n=== filter_groups_unity: distinct level_name / customer / org values ===")
# Try to find the scoping/identity columns
for col in ["customer_id", "customer", "org", "org_name", "organization", "level_1", "level_name"]:
    try:
        spark.sql(f"""
            SELECT '{col}' AS col, {col}, COUNT(*) AS n
            FROM {CATALOG}.master_data.filter_groups_unity
            GROUP BY {col} ORDER BY n DESC LIMIT 10
        """).show(10, False)
        break
    except Exception:
        continue

# ── 2. filter_values_unity schema + sample ───────────────────────────────────
print("\n=== DESCRIBE master_data.filter_values_unity ===")
spark.sql(f"DESCRIBE {CATALOG}.master_data.filter_values_unity").show(40, False)

print("\n=== filter_values_unity: row count ===")
n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.master_data.filter_values_unity").collect()[0]["n"]
print(f"  {n:,} rows")

print("\n=== filter_values_unity: sample 5 rows ===")
spark.sql(f"SELECT * FROM {CATALOG}.master_data.filter_values_unity LIMIT 5").show(5, False)

# ── 3. v_filter_group_values_kpi_flattened_unity (the view the DORA views query) ──
print("\n=== DESCRIBE master_data.v_filter_group_values_kpi_flattened_unity ===")
spark.sql(f"DESCRIBE {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity").show(40, False)

print("\n=== v_filter_group_values_kpi_flattened_unity: sample 3 rows ===")
spark.sql(f"SELECT * FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity LIMIT 3").show(3, False)

# ── 4. projects_table (may drive the Project dropdown) ───────────────────────
print("\n=== DESCRIBE master_data.projects_table ===")
spark.sql(f"DESCRIBE {CATALOG}.master_data.projects_table").show(40, False)

print("\n=== projects_table: sample 5 rows ===")
spark.sql(f"SELECT * FROM {CATALOG}.master_data.projects_table LIMIT 5").show(5, False)

print("\n=== projects_table: distinct customer / org scoping values ===")
spark.sql(f"""
    SELECT * FROM {CATALOG}.master_data.projects_table
    WHERE lower(projectName) LIKE '%insight%'
       OR lower(projectName) LIKE '%opsera%'
    LIMIT 10
""").show(10, False)

# ── 5. kpi_table — understand which KPI UUIDs map to DORA metrics ────────────
print("\n=== DESCRIBE master_data.kpi_table ===")
spark.sql(f"DESCRIBE {CATALOG}.master_data.kpi_table").show(30, False)

print("\n=== kpi_table: DORA-related KPIs ===")
spark.sql(f"""
    SELECT * FROM {CATALOG}.master_data.kpi_table
    WHERE lower(displayName) LIKE '%dora%'
       OR lower(displayName) LIKE '%deploy%'
       OR lower(displayName) LIKE '%lead time%'
       OR lower(displayName) LIKE '%mttr%'
       OR lower(displayName) LIKE '%failure%'
       OR lower(displayName) LIKE '%sdm%'
    LIMIT 20
""").show(20, False)

# ── 6. Look at an "Insights" mapping end-to-end ──────────────────────────────
# Insights appears in the Project dropdown — find its filter_groups entry
print("\n=== filter_groups_unity rows where any column contains 'Insights' ===")
try:
    # Try common column names
    for col in ["project_name", "level_3", "level_4", "name", "group_name", "application"]:
        try:
            r = spark.sql(f"""
                SELECT * FROM {CATALOG}.master_data.filter_groups_unity
                WHERE {col} LIKE '%Insights%' OR {col} LIKE '%nsight%'
                LIMIT 5
            """).collect()
            if r:
                spark.sql(f"""
                    SELECT * FROM {CATALOG}.master_data.filter_groups_unity
                    WHERE {col} LIKE '%Insights%' OR {col} LIKE '%nsight%'
                    LIMIT 5
                """).show(5, False)
                print(f"  (matched on column: {col})")
                break
        except Exception:
            continue
except Exception as e:
    print(f"  error: {e}")

# ── 7. Show CREATE TABLE for filter_groups_unity (catch any hidden columns) ──
print("\n=== SHOW CREATE TABLE master_data.filter_groups_unity ===")
spark.sql(f"SHOW CREATE TABLE {CATALOG}.master_data.filter_groups_unity").show(1, False)
