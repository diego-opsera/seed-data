# Diag — identify view-vs-table for the Sonar/Twistlock objects so we know
# where to actually INSERT.
#
# DELETE on asp_sonar_issues failed with EXPECT_TABLE_NOT_VIEW — meaning
# base_datasets.asp_sonar_issues is a view over an underlying table we need
# to write to instead. Same likely applies to asp_sonar_measures.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_5_view_check.py").read())

import json

CATALOG = "playground_prod"

CANDIDATES = [
    f"{CATALOG}.base_datasets.asp_sonar_issues",
    f"{CATALOG}.base_datasets.asp_sonar_measures",
    f"{CATALOG}.base_datasets.dependabot_scan_alert",
    f"{CATALOG}.base_datasets.code_scan_alert",          # known table — control
    f"{CATALOG}.base_datasets.secret_scan_alert",        # known table — control
    f"{CATALOG}.base_datasets.twistlock_security_issues",
    f"{CATALOG}.source_to_stage.raw_invicti_data",
]


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def is_view(fqn: str):
    """Return ('VIEW' | 'TABLE' | 'UNKNOWN', detail_text). Uses information_schema."""
    parts = fqn.split(".")
    if len(parts) != 3:
        return ("UNKNOWN", "non-3-part name")
    cat, sch, tbl = parts
    try:
        rows = spark.sql(f"""
            SELECT table_type
            FROM {cat}.information_schema.tables
            WHERE table_catalog = '{cat}'
              AND table_schema  = '{sch}'
              AND table_name    = '{tbl}'
        """).collect()
        if not rows:
            return ("UNKNOWN", "not found in information_schema.tables")
        return (rows[0]["table_type"], "")
    except Exception as e:
        return ("UNKNOWN", f"info-schema error: {e}")


def show_create(fqn: str):
    try:
        return spark.sql(f"SHOW CREATE TABLE {fqn}").collect()[0][0]
    except Exception as e:
        return f"SHOW CREATE failed: {e}"


def view_underlying(fqn: str):
    """Try to extract the underlying tables a view reads from by parsing
    SHOW CREATE TABLE output and looking for FROM/JOIN clauses."""
    ddl = show_create(fqn)
    if not isinstance(ddl, str):
        return None, ddl
    import re
    refs = re.findall(r"\b(?:FROM|JOIN)\s+([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*|`[^`]+`\.`[^`]+`\.`[^`]+`|[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)", ddl, flags=re.IGNORECASE)
    return list(dict.fromkeys(refs)), ddl


for fqn in CANDIDATES:
    print("\n" + "=" * 70)
    print(f"  {fqn}")
    print("=" * 70)
    kind, detail = is_view(fqn)
    out("table_type", {"kind": kind, "detail": detail})

    if kind == "VIEW":
        refs, ddl = view_underlying(fqn)
        out("view.references", refs)
        out("view.ddl", ddl)
    elif kind == "BASE TABLE":
        # confirm DELETE-able by counting matching demo rows (should not error)
        try:
            n = spark.sql(f"SELECT COUNT(*) AS n FROM {fqn} LIMIT 1").collect()[0]["n"]
            out("table.row_count", n)
        except Exception as e:
            out("table.row_count.error", str(e))
    else:
        out("note", f"could not classify: {detail}")
