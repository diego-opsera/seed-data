# Focused diag — base_datasets.dependabot_scan_alert
#
# Captures full schema + 2 sample rows + distinct values for the columns the
# github-security SQL files actually use. Output drives the generator's
# INSERT column list.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_3_dependabot.py").read())

import json

CATALOG = "playground_prod"
T = f"{CATALOG}.base_datasets.dependabot_scan_alert"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=10):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)}]


# 1) Full schema (all 40 cols)
schema_rows = spark.sql(f"DESCRIBE TABLE {T}").collect()
schema = {}
for r in schema_rows:
    n = r["col_name"]
    if not n or n.startswith("#") or n == "":
        break
    schema[n] = r["data_type"]
out("dependabot.schema_full", schema)

# 2) 2 real sample rows for shape reference
out("dependabot.sample_2_rows", rows(f"SELECT * FROM {T} LIMIT 2", limit=2))

# 3) Distinct values for filter / state-machine columns
for col in [
    "organization", "state", "severity",
    "dependency_scope", "dismissed_reason", "ecosystem",
    "package_name",
]:
    if col in schema:
        out(f"dependabot.distinct.{col}", rows(f"""
            SELECT {col} AS v, COUNT(*) AS n FROM {T}
            GROUP BY {col} ORDER BY n DESC LIMIT 15
        """, limit=15))
    else:
        out(f"dependabot.distinct.{col}", "column does not exist")

# 4) NULL-rate per column — tells us which fields the generator can safely skip
print("\n### dependabot.null_rate_per_column")
null_rates = {}
total = spark.sql(f"SELECT COUNT(*) n FROM {T}").collect()[0]["n"]
# Build one big SELECT to avoid 40 round-trips
nulls_select = ",\n  ".join(
    f"SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) AS `{c}`"
    for c in schema
)
nrow = spark.sql(f"SELECT {nulls_select} FROM {T}").collect()[0].asDict()
for c, n in nrow.items():
    rate = round(100.0 * (n or 0) / total, 1) if total else None
    null_rates[c] = {"nulls": n, "pct": rate}
print(json.dumps(null_rates, default=str, indent=2))

# 5) Date range — confirm what window real rows fall in (sanity for our story)
out("dependabot.date_range", rows(f"""
    SELECT
      MIN(created_at) AS min_created,
      MAX(created_at) AS max_created,
      MIN(fixed_at)   AS min_fixed,
      MAX(fixed_at)   AS max_fixed
    FROM {T}
""", limit=1))

# 6) Compare shape with already-seeded code_scan_alert (same widget) to spot
#    column overlap — generator can mirror code_scan_alert.py closely.
CSA = f"{CATALOG}.base_datasets.code_scan_alert"
csa_schema = {}
for r in spark.sql(f"DESCRIBE TABLE {CSA}").collect():
    n = r["col_name"]
    if not n or n.startswith("#") or n == "":
        break
    csa_schema[n] = r["data_type"]

shared = sorted(set(schema) & set(csa_schema))
only_dep = sorted(set(schema) - set(csa_schema))
only_csa = sorted(set(csa_schema) - set(schema))
out("col_overlap.code_scan_vs_dependabot", {
    "shared_cols": shared,
    "only_in_dependabot": only_dep,
    "only_in_code_scan_alert": only_csa,
})
