# Diag — Git Custodian source tables + KPI UUIDs
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_10_git_custodian.py").read())

import json

CATALOG = "playground_prod"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def schema(fqn):
    cols = {}
    try:
        for r in spark.sql(f"DESCRIBE TABLE {fqn}").collect():
            n = r["col_name"]
            if not n or n.startswith("#") or n == "":
                break
            cols[n] = r["data_type"]
    except Exception as e:
        return {"_error": str(e)[:200]}
    return cols


# 1. Source tables — schema + row count
for t in [
    f"{CATALOG}.source_to_stage.raw_mongo_transformed_data_gitscraper",
    f"{CATALOG}.source_to_stage.raw_mongo_transformed_data_gitscraper_issues",
]:
    print("\n" + "=" * 70)
    print(f"  {t}")
    print("=" * 70)
    s = schema(t)
    out("schema", s)
    if "_error" not in s:
        try:
            n = spark.sql(f"SELECT COUNT(*) n FROM {t}").collect()[0]["n"]
            out("row_count", n)
            sample = [r.asDict(recursive=True) for r in spark.sql(f"SELECT * FROM {t} LIMIT 1").collect()]
            out("sample_1_row", sample)
        except Exception as e:
            out("query_error", str(e)[:200])


# 2. gitcustodian_kpis — find the KPI UUIDs the dashboard widget uses
print("\n" + "=" * 70)
print("  master_data.gitcustodian_kpis")
print("=" * 70)
out("gitcustodian_kpis.schema", schema(f"{CATALOG}.master_data.gitcustodian_kpis"))
out("gitcustodian_kpis.rows", [r.asDict(recursive=True) for r in spark.sql(f"""
    SELECT * FROM {CATALOG}.master_data.gitcustodian_kpis LIMIT 30
""").collect()])


# 3. gitcustodian_dashboards
print("\n" + "=" * 70)
print("  master_data.gitcustodian_dashboards (first 5 rows)")
print("=" * 70)
out("gitcustodian_dashboards.schema", schema(f"{CATALOG}.master_data.gitcustodian_dashboards"))
out("gitcustodian_dashboards.rows", [r.asDict(recursive=True) for r in spark.sql(f"""
    SELECT * FROM {CATALOG}.master_data.gitcustodian_dashboards LIMIT 5
""").collect()])


# 4. Also pull every KPI in master_data.kpi_table whose name matches gc / git custodian / scraper
print("\n" + "=" * 70)
print("  kpi_table — Git Custodian-named entries")
print("=" * 70)
out("kpi_table.git_custodian", [r.asDict(recursive=True) for r in spark.sql(f"""
    SELECT uuid, displayName, kpi_identifier
    FROM {CATALOG}.master_data.kpi_table
    WHERE LOWER(COALESCE(displayName, '')) RLIKE 'git custodian|gitcustodian|gitscraper|gc_|git_custodian'
       OR LOWER(COALESCE(kpi_identifier, '')) RLIKE 'gc_|git_custodian|gitcustodian|gitscraper'
    ORDER BY displayName
""").collect()])
