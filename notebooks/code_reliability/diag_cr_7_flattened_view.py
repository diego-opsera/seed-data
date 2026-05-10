# Diag — dump v_filter_group_values_kpi_flattened_unity schema + a sample row
# scoped to our demo orgs. We need to know the exact KPI column name so future
# debug queries can run.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_7_flattened_view.py").read())

import json

CATALOG = "playground_prod"
V = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


# Full schema
schema = {}
for r in spark.sql(f"DESCRIBE TABLE {V}").collect():
    n = r["col_name"]
    if not n or n.startswith("#") or n == "":
        break
    schema[n] = r["data_type"]
out("flattened_view.schema", schema)
out("flattened_view.col_count", len(schema))

# 1 sample row scoped to our demo orgs (so we see real data shape)
out("flattened_view.demo_sample_1_row", [
    r.asDict(recursive=True) for r in spark.sql(f"""
        SELECT *
        FROM {V}
        WHERE level_1 IN ('Acme Corp', 'Meridian Analytics')
        LIMIT 1
    """).collect()
])

# Try common KPI column-name candidates and show which one actually exists
for candidate in ["kpi_id", "kpi_uuid", "kpi_uuids", "kpiId", "kpiIdentifier"]:
    out(f"col_exists.{candidate}", candidate in schema)
