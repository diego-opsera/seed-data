# Focused diag — base_datasets.twistlock_security_issues
#
# Captures full 40-col schema + 1 sample row + distinct values for filter
# columns the twistlock-security SQL queries actually use, plus null-rate
# per col and the cve struct shape (used by lateral view explode in the SQL).
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_8_twistlock.py").read())

import json

CATALOG = "playground_prod"
T = f"{CATALOG}.base_datasets.twistlock_security_issues"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=10):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)}]


# 1) Full schema
schema_rows = spark.sql(f"DESCRIBE TABLE {T}").collect()
schema = {}
for r in schema_rows:
    n = r["col_name"]
    if not n or n.startswith("#") or n == "":
        break
    schema[n] = r["data_type"]
out("twistlock.schema_full", schema)

# 2) 1 real sample row
out("twistlock.sample_1_row", rows(f"SELECT * FROM {T} LIMIT 1", limit=1))

# 3) Distinct values for filter / state-machine columns
for col in ["tool_identifier", "tool_data_type", "project_name",
            "image_name", "data_source", "record_inserted_by",
            "tool_data_object_type"]:
    if col in schema:
        out(f"twistlock.distinct.{col}", rows(f"""
            SELECT {col} AS v, COUNT(*) AS n FROM {T}
            GROUP BY {col} ORDER BY n DESC LIMIT 15
        """, limit=15))

# 4) NULL-rate per column
print("\n### twistlock.null_rate_per_column")
total = spark.sql(f"SELECT COUNT(*) n FROM {T}").collect()[0]["n"]
nulls_select = ",\n  ".join(
    f"SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) AS `{c}`"
    for c in schema
)
nrow = spark.sql(f"SELECT {nulls_select} FROM {T}").collect()[0].asDict()
null_rates = {
    c: {"nulls": n, "pct": round(100.0 * (n or 0) / total, 1) if total else None}
    for c, n in nrow.items()
}
print(json.dumps(null_rates, default=str, indent=2))

# 5) Date range
out("twistlock.date_range", rows(f"""
    SELECT MIN(activity_date) AS min_activity, MAX(activity_date) AS max_activity,
           MIN(source_record_insert_datetime) AS min_insert,
           MAX(source_record_insert_datetime) AS max_insert
    FROM {T}
""", limit=1))

# 6) cve array shape — confirms exploded_cve.identifier + .severity referenced by SQL
out("twistlock.cve_struct_sample", rows(f"""
    SELECT cve_item.identifier AS identifier, cve_item.severity AS severity
    FROM {T} LATERAL VIEW explode(cve) AS cve_item
    WHERE cve_item.identifier IS NOT NULL
    LIMIT 10
""", limit=10))
