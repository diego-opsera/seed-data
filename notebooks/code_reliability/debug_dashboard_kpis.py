# Debug — pull the actual KPI UUIDs configured on the user's
# "Acme Code Reliability" dashboard from master_data.master_dashboard_table.
#
# debug_filter_groups.py confirmed our wiring + data flow are correct, so
# the only remaining explanation for empty widgets is that the dashboard's
# widgets are bound to KPI UUIDs DIFFERENT from the ones in
# kpiIdentifierConfig.json. We need the actual UUIDs the widgets are using.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/debug_dashboard_kpis.py").read())

import json
import re

CATALOG = "playground_prod"
DASHBOARD_UUID = "454b685d-0207-4130-85d9-38f8f606eeca"
DASHBOARD_TABLE = f"{CATALOG}.master_data.master_dashboard_table"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=200):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)}]


# ── 1. Show the dashboard row's schema (so we know which column holds widgets)
print("\n" + "=" * 70)
print("  1. master_dashboard_table schema")
print("=" * 70)
schema = {}
try:
    for r in spark.sql(f"DESCRIBE TABLE {DASHBOARD_TABLE}").collect():
        n = r["col_name"]
        if not n or n.startswith("#") or n == "":
            break
        schema[n] = r["data_type"]
    out("master_dashboard_table.schema", schema)
except Exception as e:
    out("ERROR", str(e))


# ── 2. The full row for our specific dashboard
print("\n" + "=" * 70)
print(f"  2. Full row for dashboard UUID {DASHBOARD_UUID}")
print("=" * 70)
out("dashboard.row", rows(f"""
    SELECT * FROM {DASHBOARD_TABLE}
    WHERE uuid = '{DASHBOARD_UUID}'
"""))


# ── 3. If there's a column with widget config (often JSON-stringified),
#       attempt to extract every UUID-shaped string from it.
print("\n" + "=" * 70)
print("  3. UUID-shaped tokens found in the dashboard's stringified columns")
print("=" * 70)

uuid_re = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)

dashboard_rows = []
try:
    dashboard_rows = spark.sql(
        f"SELECT * FROM {DASHBOARD_TABLE} WHERE uuid = '{DASHBOARD_UUID}'"
    ).collect()
except Exception as e:
    out("query_error", str(e))

if dashboard_rows:
    row_dict = dashboard_rows[0].asDict(recursive=True)
    found_uuids = {}
    for col, val in row_dict.items():
        if val is None:
            continue
        s = str(val)
        matches = uuid_re.findall(s)
        if matches:
            # Drop the dashboard's own UUID from results (it'll be in `uuid` col)
            uniq = sorted(set(m for m in matches if m.lower() != DASHBOARD_UUID.lower()))
            if uniq:
                found_uuids[col] = uniq
    out("uuids_per_column", found_uuids)
else:
    out("dashboard.found", False)


# ── 4. Cross-check: of the UUIDs found, which ones are bound to our seeded
#       project_name in the flattened view? If none, we need to wire them.
print("\n" + "=" * 70)
print("  4. Which dashboard-config UUIDs are present in our flattened view?")
print("=" * 70)

if dashboard_rows:
    all_uuids = sorted({u for col_uuids in found_uuids.values() for u in col_uuids})
    if all_uuids:
        in_clause = ", ".join(f"'{u}'" for u in all_uuids)
        wired = rows(f"""
            SELECT DISTINCT kpi_uuids
            FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
            WHERE level_3 IN ('demo-acme-corp', 'demo-meridian')
              AND kpi_uuids IN ({in_clause})
        """, limit=200)
        wired_set = {r["kpi_uuids"] for r in wired if "kpi_uuids" in r}
        missing = sorted(set(all_uuids) - wired_set)
        out("dashboard_uuids.wired_to_our_orgs", sorted(wired_set))
        out("dashboard_uuids.missing_from_our_filter_wiring", missing)
        out("dashboard_uuids.total_count", len(all_uuids))


# ── 5. As a fallback, look up KPI metadata from master_data.kpi_table for
#       any UUID found in the dashboard row, so we know what each widget IS.
print("\n" + "=" * 70)
print("  5. Names of each KPI UUID found on the dashboard (from kpi_table)")
print("=" * 70)

if dashboard_rows and all_uuids:
    in_clause = ", ".join(f"'{u}'" for u in all_uuids)
    out("kpi_metadata", rows(f"""
        SELECT uuid, displayName, kpi_identifier
        FROM {CATALOG}.master_data.kpi_table
        WHERE uuid IN ({in_clause})
        ORDER BY displayName
    """, limit=100))
