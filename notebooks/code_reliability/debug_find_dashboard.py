# Debug — find where dashboard UUID 454b685d-0207-4130-85d9-38f8f606eeca lives.
#
# master_dashboard_table doesn't have it. Check every table in master_data
# for a column or row containing this UUID. If none in Databricks, the
# dashboard config is in MongoDB and we'd need a different approach.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/debug_find_dashboard.py").read())

import json

CATALOG = "playground_prod"
DASHBOARD_UUID = "454b685d-0207-4130-85d9-38f8f606eeca"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


# 1. List every table in master_data
tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {CATALOG}.master_data").collect()]
out("master_data.tables", tables)


# 2. List every table whose name matches dashboard / kpi / widget
candidates = [t for t in tables
              if any(kw in t.lower() for kw in ["dashboard", "kpi", "widget", "user", "saved"])]
out("candidate_tables", candidates)


# 3. Try LIKE search across each candidate's string columns for the UUID
print("\n" + "=" * 70)
print(f"  3. Searching candidate tables for UUID {DASHBOARD_UUID}")
print("=" * 70)

hits = {}
for t in candidates:
    fqn = f"{CATALOG}.master_data.{t}"
    try:
        # Get string + array columns
        cols = []
        for r in spark.sql(f"DESCRIBE TABLE {fqn}").collect():
            n = r["col_name"]
            d = r["data_type"]
            if not n or n.startswith("#") or n == "":
                break
            if d == "string" or d.startswith("array<string>"):
                cols.append((n, d))

        # Build a single OR'd predicate
        preds = []
        for n, d in cols:
            if d == "string":
                preds.append(f"`{n}` LIKE '%{DASHBOARD_UUID}%'")
            elif d.startswith("array<string>"):
                preds.append(f"array_contains(`{n}`, '{DASHBOARD_UUID}')")
        if not preds:
            continue
        where = " OR ".join(preds)
        n = spark.sql(f"SELECT COUNT(*) AS n FROM {fqn} WHERE {where}").collect()[0]["n"]
        if n > 0:
            hits[t] = {"matching_rows": n, "predicate": where[:200] + "..." if len(where) > 200 else where}
    except Exception as e:
        hits[t] = {"error": str(e)[:200]}

out("uuid_search_hits", hits)


# 4. If we hit something, dump the matching row(s) for inspection
print("\n" + "=" * 70)
print("  4. Matching rows from any hit tables")
print("=" * 70)

for t, info in hits.items():
    if "matching_rows" not in info:
        continue
    fqn = f"{CATALOG}.master_data.{t}"
    try:
        # Re-build predicate without truncation
        cols = []
        for r in spark.sql(f"DESCRIBE TABLE {fqn}").collect():
            n = r["col_name"]
            d = r["data_type"]
            if not n or n.startswith("#") or n == "":
                break
            if d == "string" or d.startswith("array<string>"):
                cols.append((n, d))
        preds = []
        for n, d in cols:
            if d == "string":
                preds.append(f"`{n}` LIKE '%{DASHBOARD_UUID}%'")
            elif d.startswith("array<string>"):
                preds.append(f"array_contains(`{n}`, '{DASHBOARD_UUID}')")
        where = " OR ".join(preds)
        rows_data = spark.sql(f"SELECT * FROM {fqn} WHERE {where} LIMIT 3").collect()
        out(f"hit.{t}.rows", [r.asDict(recursive=True) for r in rows_data])
    except Exception as e:
        out(f"hit.{t}.error", str(e))
