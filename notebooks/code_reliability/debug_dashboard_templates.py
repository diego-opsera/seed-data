# Debug — find code-reliability dashboard TEMPLATES (not the user's copy).
#
# The user's dashboard 454b685d-... isn't in any Databricks table — it's
# stored in MongoDB as a user-customized clone. But the original TEMPLATE
# it was cloned from, plus other code-reliability dashboards in the
# system, ARE in master_data. Their `kpis` arrays show the canonical
# widget→KPI bindings.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/debug_dashboard_templates.py").read())

import json

CATALOG = "playground_prod"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=200):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)}]


# ── 1. Code-reliability shaped dashboards in master_dashboard_table
print("\n" + "=" * 70)
print("  1. Dashboards whose displayName/description mentions code reliability")
print("=" * 70)
out("master_dashboard_table.code_reliability_dashboards", rows(f"""
    SELECT uuid, displayName, description, type, kpis, sqlPath, createdBy
    FROM {CATALOG}.master_data.master_dashboard_table
    WHERE LOWER(COALESCE(displayName, '')) RLIKE 'reliab|sonar|twistlock|coverage|defect|web app|web application'
       OR LOWER(COALESCE(description, '')) RLIKE 'reliab|sonar|twistlock|coverage|defect|web app|web application'
    ORDER BY displayName
""", limit=50))


# ── 2. velocity_dashboards (alternate dashboard storage)
print("\n" + "=" * 70)
print("  2. velocity_dashboards mentioning reliability/sonar/twistlock")
print("=" * 70)
try:
    schema_rows = spark.sql(f"DESCRIBE TABLE {CATALOG}.master_data.velocity_dashboards").collect()
    schema = {}
    for r in schema_rows:
        n = r["col_name"]
        if not n or n.startswith("#") or n == "":
            break
        schema[n] = r["data_type"]
    out("velocity_dashboards.schema", schema)

    string_cols = [n for n, d in schema.items() if d == "string"]
    if string_cols:
        preds = " OR ".join(
            f"LOWER(COALESCE(`{c}`, '')) RLIKE 'reliab|sonar|twistlock|coverage|defect|web app'"
            for c in string_cols
        )
        out("velocity_dashboards.matching_rows", rows(f"""
            SELECT * FROM {CATALOG}.master_data.velocity_dashboards
            WHERE {preds}
            LIMIT 10
        """))
except Exception as e:
    out("velocity_dashboards.error", str(e))


# ── 3. ALL kpi_table entries whose name screams "Code Reliability widget"
print("\n" + "=" * 70)
print("  3. kpi_table — every Sonar Ratings / Twistlock / Web App / Defect Density entry")
print("=" * 70)
out("kpi_table.matching_rows", rows(f"""
    SELECT uuid, displayName, kpi_identifier, createdBy, createdAt
    FROM {CATALOG}.master_data.kpi_table
    WHERE LOWER(COALESCE(displayName, '')) RLIKE 'sonar.*rating|twistlock.*security|web.*app.*security|defect.*density|coverage.*overview|code.*coverage|sonar.*defect|sonar.*overview'
       OR LOWER(COALESCE(kpi_identifier, '')) RLIKE 'sonar_rat|twistlock|web_app|invicti|defect_density|coverage_overview|sonar_overview'
    ORDER BY displayName
""", limit=50))


# ── 4. kpi_table_2 (alternate KPI store)
print("\n" + "=" * 70)
print("  4. kpi_table_2 — same matchers")
print("=" * 70)
try:
    out("kpi_table_2.matching_rows", rows(f"""
        SELECT uuid, displayName, kpi_identifier, createdBy
        FROM {CATALOG}.master_data.kpi_table_2
        WHERE LOWER(COALESCE(displayName, '')) RLIKE 'sonar|twistlock|web.*app|defect|coverage|reliab'
           OR LOWER(COALESCE(kpi_identifier, '')) RLIKE 'sonar|twistlock|web_app|invicti|defect|coverage|reliab'
        ORDER BY displayName
    """, limit=50))
except Exception as e:
    out("kpi_table_2.error", str(e))


# ── 5. Show every UUID currently bound to OUR Acme filter group through
#       the flattened view — sanity check what we already have in place
print("\n" + "=" * 70)
print("  5. ALL kpi_uuids surfaced for Acme via flattened view (after shotgun)")
print("=" * 70)
out("flattened.acme_all_kpi_uuids", rows(f"""
    SELECT DISTINCT kpi_uuids, tool_type
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = 'demo-acme-corp'
      AND project_name IS NOT NULL
      AND size(project_name) > 0
      AND array_contains(project_name, 'backend')
    ORDER BY kpi_uuids
""", limit=200))


# ── 6. Same for Meridian
print("\n" + "=" * 70)
print("  6. ALL kpi_uuids surfaced for Meridian via flattened view")
print("=" * 70)
out("flattened.meridian_all_kpi_uuids", rows(f"""
    SELECT DISTINCT kpi_uuids, tool_type
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = 'demo-meridian'
      AND project_name IS NOT NULL
      AND size(project_name) > 0
      AND array_contains(project_name, 'data-platform')
    ORDER BY kpi_uuids
""", limit=200))
