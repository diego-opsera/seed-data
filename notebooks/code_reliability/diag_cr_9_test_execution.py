# Diag — confirm v_itsm_issues_current has the columns Test Execution Metrics needs.
# If columns missing → widget is unsupported in this catalog regardless of seed.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_9_test_execution.py").read())

import json

CATALOG = "playground_prod"
V = f"{CATALOG}.base_datasets.v_itsm_issues_current"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


schema_rows = spark.sql(f"DESCRIBE TABLE {V}").collect()
schema = {}
for r in schema_rows:
    n = r["col_name"]
    if not n or n.startswith("#") or n == "":
        break
    schema[n] = r["data_type"]
out("v_itsm_issues_current.col_count", len(schema))

needed = {
    "issue_type": "filter on 'Test Execution' / 'Test'",
    "fix_version": "ARRAY filter for fixVersions",
    "test_execution_tickets": "ARRAY of structs with .testKey",
    "test_execution_status":  "STRUCT with .statuses ARRAY",
    "issue_project": "JOIN to filter_groups.project_name",
    "issue_created": "date filter",
    "issue_key": "identity",
}
for col, why in needed.items():
    out(f"col.{col}", {"present": col in schema, "type": schema.get(col, "MISSING"), "why": why})

# Are there ANY rows with issue_type = 'Test Execution' or 'Test' in the catalog?
out("rowcount.test_execution",
    spark.sql(f"SELECT COUNT(*) AS n FROM {V} WHERE issue_type='Test Execution'").collect()[0]["n"])
out("rowcount.test",
    spark.sql(f"SELECT COUNT(*) AS n FROM {V} WHERE issue_type='Test'").collect()[0]["n"])

# If any rows exist, sample one of each so we know the struct shape
if "test_execution_tickets" in schema:
    out("sample.test_execution_with_tickets", [r.asDict(recursive=True) for r in spark.sql(f"""
        SELECT issue_key, issue_project, fix_version, issue_type, test_execution_tickets
        FROM {V}
        WHERE issue_type = 'Test Execution'
          AND test_execution_tickets IS NOT NULL
          AND size(test_execution_tickets) > 0
        LIMIT 1
    """).collect()])

if "test_execution_status" in schema:
    out("sample.test_with_status", [r.asDict(recursive=True) for r in spark.sql(f"""
        SELECT issue_key, issue_project, issue_type, test_execution_status
        FROM {V}
        WHERE issue_type = 'Test'
          AND test_execution_status IS NOT NULL
        LIMIT 1
    """).collect()])
