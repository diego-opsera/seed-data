# Diag — does source_to_stage.junit_test_suite_report exist?
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_11_junit.py").read())

import json

CATALOG = "playground_prod"
T = f"{CATALOG}.source_to_stage.junit_test_suite_report"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


try:
    schema = {}
    for r in spark.sql(f"DESCRIBE TABLE {T}").collect():
        n = r["col_name"]
        if not n or n.startswith("#") or n == "":
            break
        schema[n] = r["data_type"]
    out("junit_test_suite_report.exists", True)
    out("junit_test_suite_report.schema", schema)
    n = spark.sql(f"SELECT COUNT(*) n FROM {T}").collect()[0]["n"]
    out("junit_test_suite_report.row_count", n)
    if n > 0:
        out("junit_test_suite_report.sample_1_row",
            [r.asDict(recursive=True) for r in spark.sql(f"SELECT * FROM {T} LIMIT 1").collect()])
except Exception as e:
    out("junit_test_suite_report.exists", False)
    out("error", str(e)[:300])

# Also: KPI UUIDs related to JUnit
out("kpi_table.junit_entries", [r.asDict(recursive=True) for r in spark.sql(f"""
    SELECT uuid, displayName, kpi_identifier
    FROM {CATALOG}.master_data.kpi_table
    WHERE LOWER(COALESCE(displayName, '')) RLIKE 'junit'
       OR LOWER(COALESCE(kpi_identifier, '')) RLIKE 'junit'
""").collect()])
