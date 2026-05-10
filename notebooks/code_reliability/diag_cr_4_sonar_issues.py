# Focused diag — base_datasets.asp_sonar_issues
#
# Captures full schema + 2 sample rows + distinct values for filter columns
# the sonar-ratings + sonarqube_def_dens SQL queries actually use, plus
# combination distributions (type × severity × status) so the generator
# matches realistic shapes.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_4_sonar_issues.py").read())

import json

CATALOG = "playground_prod"
T = f"{CATALOG}.base_datasets.asp_sonar_issues"


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
out("sonar_issues.schema_full", schema)

# 2) 2 real sample rows
out("sonar_issues.sample_2_rows", rows(f"SELECT * FROM {T} LIMIT 2", limit=2))

# 3) Distinct values for filter / state-machine columns
for col in [
    "org_name", "project", "branch", "type", "severity",
    "status", "resolution", "rule", "author",
]:
    if col in schema:
        out(f"sonar_issues.distinct.{col}", rows(f"""
            SELECT {col} AS v, COUNT(*) AS n FROM {T}
            GROUP BY {col} ORDER BY n DESC LIMIT 15
        """, limit=15))
    else:
        out(f"sonar_issues.distinct.{col}", "column does not exist")

# 4) Combination distributions — what shape does a realistic row take?
out("sonar_issues.dist.type_x_severity", rows(f"""
    SELECT type, severity, COUNT(*) AS n FROM {T}
    GROUP BY type, severity ORDER BY type, n DESC
""", limit=40))

out("sonar_issues.dist.type_x_status", rows(f"""
    SELECT type, status, COUNT(*) AS n FROM {T}
    GROUP BY type, status ORDER BY type, n DESC
""", limit=40))

# 5) NULL-rate per column
print("\n### sonar_issues.null_rate_per_column")
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

# 6) Date range — confirm the column the SQL filters on
out("sonar_issues.date_range", rows(f"""
    SELECT
      MIN(update_date) AS min_update,
      MAX(update_date) AS max_update,
      MIN(creation_date) AS min_create,
      MAX(creation_date) AS max_create
    FROM {T}
""", limit=1))

# 7) Per-project issue counts — useful to know how dense a "project_name" can be
out("sonar_issues.top_projects", rows(f"""
    SELECT project, branch, COUNT(*) AS n FROM {T}
    GROUP BY project, branch ORDER BY n DESC LIMIT 10
""", limit=10))

# 8) source_record_insert_datetime sanity — sonar_ratings_overview.sql joins
#    asp_sonar_issues to asp_sonar_measures on this exact column. If our seeded
#    issues' source_record_insert_datetime doesn't match the seeded measures
#    row's, the ratings widget shows nothing.
if "source_record_insert_datetime" in schema:
    out("sonar_issues.distinct.source_record_insert_datetime_top10", rows(f"""
        SELECT source_record_insert_datetime AS v, COUNT(*) AS n FROM {T}
        GROUP BY source_record_insert_datetime ORDER BY n DESC LIMIT 10
    """, limit=10))
