# Diag — confirm underlying-table schemas for the Sonar views.
#
# Run after diag_cr_5 confirmed asp_sonar_issues + asp_sonar_measures are
# views. Captures the schema, row count, and one sample row for each
# underlying table so we can:
#   1) sanity-check the asp_sonar_issues generator's INSERT column list
#   2) plan the asp_sonar_measures generator (next).
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_6_underlying_sonar.py").read())

import json

CATALOG = "playground_prod"

TABLES = [
    f"{CATALOG}.source_to_stage.raw_sonar_type_data_branchwise",
    f"{CATALOG}.source_to_stage.raw_sonar_metric_split_data_branchwise",
    f"{CATALOG}.source_to_stage.raw_sonar_project_branch_list",
]


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def schema(fqn):
    cols = {}
    for r in spark.sql(f"DESCRIBE TABLE {fqn}").collect():
        n = r["col_name"]
        if not n or n.startswith("#") or n == "":
            break
        cols[n] = r["data_type"]
    return cols


for fqn in TABLES:
    print("\n" + "=" * 70)
    print(f"  {fqn}")
    print("=" * 70)
    try:
        s = schema(fqn)
        out("schema", s)
        out("col_count", len(s))
        n = spark.sql(f"SELECT COUNT(*) AS n FROM {fqn}").collect()[0]["n"]
        out("row_count", n)
        if n > 0:
            sample = [r.asDict(recursive=True) for r in spark.sql(f"SELECT * FROM {fqn} LIMIT 1").collect()]
            out("sample_1_row", sample)
    except Exception as e:
        out("ERROR", str(e))


# Cross-check raw_sonar_type_data_branchwise schema vs the asp_sonar_issues
# view's column list — confirms `select *` view is a clean passthrough.
print("\n" + "=" * 70)
print("  CROSS-CHECK: view cols == underlying-table cols?")
print("=" * 70)

view_cols = list(schema(f"{CATALOG}.base_datasets.asp_sonar_issues").keys())
table_cols = list(schema(f"{CATALOG}.source_to_stage.raw_sonar_type_data_branchwise").keys())
out("asp_sonar_issues.view_cols", view_cols)
out("raw_sonar_type_data_branchwise.table_cols", table_cols)
out("only_in_view",       sorted(set(view_cols) - set(table_cols)))
out("only_in_underlying", sorted(set(table_cols) - set(view_cols)))
