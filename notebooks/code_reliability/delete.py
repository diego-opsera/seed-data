# Code Reliability dashboard — unified delete (Acme + Meridian in one place)
#
# Every delete is tightly scoped to a demo org identifier — never touches
# real customer rows. Add new (table, predicate) entries here as we add
# generators for sonar, twistlock, was, etc.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/delete.py").read())

CATALOG = "playground_prod"

_deletes = [
    # dependabot_scan_alert — scoped by `organization` (matches generator output)
    ("base_datasets.dependabot_scan_alert",
     "organization IN ('demo-acme-direct', 'demo-meridian')"),
]

for table, predicate in _deletes:
    fqn = f"{CATALOG}.{table}"
    try:
        n = spark.sql(f"SELECT COUNT(*) FROM {fqn} WHERE {predicate}").collect()[0][0]
        spark.sql(f"DELETE FROM {fqn} WHERE {predicate}")
        print(f"{table}: deleted {n} rows")
    except Exception as e:
        print(f"{table}: ERROR — {e}")
