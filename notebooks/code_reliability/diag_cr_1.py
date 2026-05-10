# Diagnostic round 1 — Code Reliability dashboard tables
#
# Confirms each base table referenced by the dashboard's API SQL exists in
# playground_prod, captures the schema, and surfaces any existing rows.
#
# Output is JSON/compact dicts (no Spark .show() borders) so it pastes back
# cleanly. Same pattern as notebooks/dora/diag_dora_4.py.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_1.py").read())

import json

CATALOG = "playground_prod"

TABLES = [
    # (label, fully-qualified name, candidate scope columns to probe)
    ("asp_sonar_measures",        f"{CATALOG}.base_datasets.asp_sonar_measures",
        ["org_name", "project_name", "branch", "last_analysis_date"]),
    ("asp_sonar_issues",          f"{CATALOG}.base_datasets.asp_sonar_issues",
        ["org_name", "project", "type", "severity", "status", "update_date"]),
    ("dependabot_scan_alert",     f"{CATALOG}.base_datasets.dependabot_scan_alert",
        ["organization", "repository_name", "state", "severity", "created_at"]),
    ("twistlock_security_issues", f"{CATALOG}.base_datasets.twistlock_security_issues",
        ["tool_identifier", "tool_data_type", "project_name", "activity_date"]),
    ("raw_invicti_data",          f"{CATALOG}.source_to_stage.raw_invicti_data",
        ["WebsiteName", "WebsiteId", "State", "InitiatedAt"]),
    ("raw_invicti_all_issues",    f"{CATALOG}.source_to_stage.raw_invicti_all_issues",
        ["WebsiteId", "Severity", "State"]),
]


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def table_exists(fqn):
    try:
        spark.sql(f"DESCRIBE TABLE {fqn}").collect()
        return True
    except Exception as e:
        return False


def schema(fqn):
    rows = spark.sql(f"DESCRIBE TABLE {fqn}").collect()
    cols = {}
    for r in rows:
        n = r["col_name"]
        if not n or n.startswith("#") or n == "":
            break
        cols[n] = r["data_type"]
    return cols


def safe_count(fqn):
    try:
        return spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").collect()[0]["n"]
    except Exception as e:
        return f"ERROR: {e}"


def safe_sample(fqn, n=3):
    try:
        rows = spark.sql(f"SELECT * FROM {fqn} LIMIT {n}").collect()
        return [r.asDict(recursive=True) for r in rows]
    except Exception as e:
        return [{"error": str(e)}]


def safe_distinct(fqn, col, limit=15):
    try:
        rows = spark.sql(
            f"SELECT {col} AS v, COUNT(*) AS n FROM {fqn} "
            f"GROUP BY {col} ORDER BY n DESC LIMIT {limit}"
        ).collect()
        return [{"value": r["v"], "count": r["n"]} for r in rows]
    except Exception as e:
        return [{"error": str(e)}]


# ── Per-table inspection ────────────────────────────────────────────────────

summary = {}

for label, fqn, scope_cols in TABLES:
    print("\n" + "=" * 70)
    print(f"  {label}  →  {fqn}")
    print("=" * 70)

    exists = table_exists(fqn)
    summary[label] = {"fqn": fqn, "exists": exists}

    if not exists:
        out(f"{label}.exists", False)
        continue

    s = schema(fqn)
    out(f"{label}.schema", s)
    summary[label]["column_count"] = len(s)

    n = safe_count(fqn)
    out(f"{label}.row_count_total", n)
    summary[label]["row_count"] = n

    out(f"{label}.sample_rows", safe_sample(fqn, n=3))

    for col in scope_cols:
        if col in s:
            out(f"{label}.distinct.{col}", safe_distinct(fqn, col, limit=15))


# ── Existing demo-* presence check ───────────────────────────────────────────
# Surfaces any rows that already have a "demo" tag — would be a surprise we'd
# want to know about before any insert.
print("\n" + "=" * 70)
print("  EXISTING demo-* row presence (sanity check)")
print("=" * 70)

demo_probes = [
    ("asp_sonar_measures",        "lower(coalesce(org_name,'')) RLIKE 'demo|acme|meridian'"),
    ("asp_sonar_issues",          "lower(coalesce(org_name,'')) RLIKE 'demo|acme|meridian'"),
    ("dependabot_scan_alert",     "lower(coalesce(organization,'')) RLIKE 'demo|acme|meridian'"),
    ("twistlock_security_issues", "exists(project_name, p -> lower(p) RLIKE 'demo|acme|meridian')"),
    ("raw_invicti_data",          "lower(coalesce(WebsiteName,'')) RLIKE 'demo|acme|meridian'"),
    ("raw_invicti_all_issues",    "lower(coalesce(WebsiteId,'')) RLIKE 'demo|acme|meridian'"),
]

demo_counts = {}
for label, where in demo_probes:
    fqn = next((t[1] for t in TABLES if t[0] == label), None)
    if not fqn or not summary.get(label, {}).get("exists"):
        demo_counts[label] = "table missing"
        continue
    try:
        n = spark.sql(f"SELECT COUNT(*) AS n FROM {fqn} WHERE {where}").collect()[0]["n"]
        demo_counts[label] = n
    except Exception as e:
        demo_counts[label] = f"ERROR: {e}"

out("demo_row_presence", demo_counts)


# ── Final summary ────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("  SUMMARY")
print("=" * 70)
out("summary", summary)
