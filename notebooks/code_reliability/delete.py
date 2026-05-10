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
    # Code-reliability filter wiring — must be cleaned BEFORE dora/delete.py
    # would also catch them by created_by, but we own them so we delete them
    # explicitly with our own tag.
    ("master_data.filter_values_unity",
     "created_by = 'seed-data-cr@demo.io'"),
    # dependabot_scan_alert — scoped by `organization` (matches generator output)
    ("base_datasets.dependabot_scan_alert",
     "organization IN ('demo-acme-direct', 'demo-meridian')"),
    # raw_sonar_type_data_branchwise — underlying table for the
    # base_datasets.asp_sonar_issues view. Scoped by record_inserted_by
    # (distinct from real 'sp_opsera_prodqa_de' tag).
    ("source_to_stage.raw_sonar_type_data_branchwise",
     "record_inserted_by IN ('seed-data', 'seed-data-meridian')"),
]

for table, predicate in _deletes:
    fqn = f"{CATALOG}.{table}"
    try:
        n = spark.sql(f"SELECT COUNT(*) FROM {fqn} WHERE {predicate}").collect()[0][0]
        spark.sql(f"DELETE FROM {fqn} WHERE {predicate}")
        print(f"{table}: deleted {n} rows")
    except Exception as e:
        print(f"{table}: ERROR — {e}")
