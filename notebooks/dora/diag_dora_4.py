# Diagnostic round 4: filter_groups_unity + v_filter_group_values_kpi_flattened_unity
# Output is JSON/compact text — machine-readable for Claude, not pretty-printed tables
# Run via exec(notebook.read()) in the Databricks notebook

import json

CATALOG = "playground_prod"

def sql(q):
    return spark.sql(q)

def rows(q, limit=10):
    return [r.asDict() for r in sql(q).limit(limit).collect()]

def schema(table):
    return {r["col_name"]: r["data_type"] for r in sql(f"DESCRIBE {table}").collect() if r["col_name"] and not r["col_name"].startswith("#")}

def ddl(table):
    return sql(f"SHOW CREATE TABLE {table}").collect()[0][0]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

# ── filter_groups_unity ──────────────────────────────────────────────────────

FGU = f"{CATALOG}.master_data.filter_groups_unity"

out("fgu.schema", schema(FGU))

out("fgu.row_count", sql(f"SELECT COUNT(*) AS n FROM {FGU}").collect()[0]["n"])

out("fgu.sample_10_rows", rows(f"SELECT * FROM {FGU}", 10))

out("fgu.distinct_level_1", rows(f"""
    SELECT level_1, COUNT(*) AS n FROM {FGU}
    GROUP BY level_1 ORDER BY n DESC
""", 20))

out("fgu.hierarchy_sample", rows(f"""
    SELECT level_1, level_2, level_3, level_4, labels, id FROM {FGU}
    ORDER BY level_1, level_2, level_3, level_4, labels
""", 20))

out("fgu.demo_acme_opsera_rows", rows(f"""
    SELECT * FROM {FGU}
    WHERE lower(concat_ws(' ', level_1, level_2, level_3, level_4, labels))
          RLIKE 'demo|acme|opsera'
""", 20))

out("fgu.ddl", ddl(FGU))

# ── v_filter_group_values_kpi_flattened_unity ────────────────────────────────

FGVF = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"

out("fgvf.schema", schema(FGVF))

out("fgvf.row_count", sql(f"SELECT COUNT(*) AS n FROM {FGVF}").collect()[0]["n"])

out("fgvf.sample_5_rows", rows(f"SELECT * FROM {FGVF}", 5))

out("fgvf.distinct_level_1", rows(f"""
    SELECT level_1, COUNT(*) AS n FROM {FGVF}
    GROUP BY level_1 ORDER BY n DESC
""", 20))

out("fgvf.demo_acme_rows", rows(f"""
    SELECT * FROM {FGVF}
    WHERE lower(concat_ws(' ', level_1, level_2, level_3, level_4, labels))
          RLIKE 'demo|acme'
""", 10))

out("fgvf.ddl", ddl(FGVF))
