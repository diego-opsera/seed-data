# Diagnostic: check SnapLogic tables in playground_prod
# Run each cell via exec(notebook.read()) in the Databricks notebook

CATALOG = "playground_prod"

# ── 1. Do the three raw tables exist? ───────────────────────────────────────
for tbl in [
    "source_to_stage.raw_snaplogic_snaplex",
    "source_to_stage.raw_snaplogic_snaplex_nodes",
    "source_to_stage.raw_snaplogic_activities",
]:
    try:
        n = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{tbl}").collect()[0]["n"]
        print(f"EXISTS  {tbl}  ({n:,} rows)")
    except Exception as e:
        print(f"MISSING {tbl}  — {e}")

# ── 2. Schemas ───────────────────────────────────────────────────────────────
print("\n=== raw_snaplogic_snaplex columns ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.source_to_stage.raw_snaplogic_snaplex").show(50, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== raw_snaplogic_snaplex_nodes columns ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.source_to_stage.raw_snaplogic_snaplex_nodes").show(50, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== raw_snaplogic_activities columns ===")
try:
    spark.sql(f"DESCRIBE {CATALOG}.source_to_stage.raw_snaplogic_activities").show(50, False)
except Exception as e:
    print(f"  error: {e}")

# ── 3. Sample rows ───────────────────────────────────────────────────────────
print("\n=== raw_snaplogic_snaplex sample (5 rows) ===")
try:
    spark.sql(f"SELECT * FROM {CATALOG}.source_to_stage.raw_snaplogic_snaplex LIMIT 5").show(5, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== raw_snaplogic_snaplex_nodes sample (5 rows) ===")
try:
    spark.sql(f"SELECT * FROM {CATALOG}.source_to_stage.raw_snaplogic_snaplex_nodes LIMIT 5").show(5, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== raw_snaplogic_activities sample (5 rows) ===")
try:
    spark.sql(f"SELECT * FROM {CATALOG}.source_to_stage.raw_snaplogic_activities LIMIT 5").show(5, False)
except Exception as e:
    print(f"  error: {e}")

# ── 4. Distinct org / environment / location values ──────────────────────────
print("\n=== distinct org values in raw_snaplogic_snaplex ===")
try:
    spark.sql(f"""
        SELECT org, COUNT(*) AS n
        FROM {CATALOG}.source_to_stage.raw_snaplogic_snaplex
        GROUP BY org ORDER BY n DESC LIMIT 20
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== distinct environment values in raw_snaplogic_snaplex ===")
try:
    spark.sql(f"""
        SELECT environment, COUNT(*) AS n
        FROM {CATALOG}.source_to_stage.raw_snaplogic_snaplex
        GROUP BY environment ORDER BY n DESC LIMIT 20
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== distinct org values in raw_snaplogic_snaplex_nodes ===")
try:
    spark.sql(f"""
        SELECT org, COUNT(*) AS n
        FROM {CATALOG}.source_to_stage.raw_snaplogic_snaplex_nodes
        GROUP BY org ORDER BY n DESC LIMIT 20
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== distinct org_label values in raw_snaplogic_activities ===")
try:
    spark.sql(f"""
        SELECT org_label, COUNT(*) AS n
        FROM {CATALOG}.source_to_stage.raw_snaplogic_activities
        GROUP BY org_label ORDER BY n DESC LIMIT 20
    """).show(20, False)
except Exception as e:
    print(f"  error: {e}")

# ── 5. Check if any org already contains 'demo' data ────────────────────────
print("\n=== existing demo rows in raw_snaplogic_snaplex ===")
try:
    spark.sql(f"""
        SELECT * FROM {CATALOG}.source_to_stage.raw_snaplogic_snaplex
        WHERE org LIKE '%demo%' OR label LIKE '%demo%'
        LIMIT 10
    """).show(10, False)
except Exception as e:
    print(f"  error: {e}")

# ── 6. Any tool/org mapping tables that reference snaplogic? ─────────────────
print("\n=== tables in playground_prod.master_data (scan for snaplogic mapping) ===")
try:
    spark.sql(f"SHOW TABLES IN {CATALOG}.master_data").show(50, False)
except Exception as e:
    print(f"  error: {e}")

print("\n=== tables in playground_prod.source_to_stage ===")
try:
    spark.sql(f"SHOW TABLES IN {CATALOG}.source_to_stage").show(100, False)
except Exception as e:
    print(f"  error: {e}")
