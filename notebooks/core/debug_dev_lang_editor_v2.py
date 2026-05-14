# Verify the "_new" version of the IDE org-level table — that's where the
# dashboard reads from when isNewVersion=true (controlled by a feature flag
# on the API side). See insights-summary.controller.js:2119–2123.
#
# Our generator only writes to the legacy table; if the running API has the
# feature flag on, our data is invisible to this dashboard.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/core/debug_dev_lang_editor_v2.py").read())

CATALOG = "playground_prod"
NEW_TABLE = "github_copilot_metrics_ide_org_level_new"
OLD_TABLE = "github_copilot_metrics_ide_org_level"

print()
print("=" * 78)
print(f"_new table existence + content: {CATALOG}.base_datasets.{NEW_TABLE}")
print("=" * 78)

exists = False
try:
    exists = spark.catalog.tableExists(f"{CATALOG}.base_datasets.{NEW_TABLE}")
    print(f"\nexists: {exists}")
except Exception as e:
    print(f"existence check error: {e}")

if exists:
    print("\n-- _new schema --")
    spark.sql(f"DESCRIBE {CATALOG}.base_datasets.{NEW_TABLE}").show(60, truncate=False)

    print("\n-- _new row count and org breakdown --")
    spark.sql(f"""
      SELECT org_name, COUNT(*) AS rows,
             MIN(copilot_usage_date) AS min_date,
             MAX(copilot_usage_date) AS max_date
      FROM {CATALOG}.base_datasets.{NEW_TABLE}
      GROUP BY org_name
      ORDER BY rows DESC
      LIMIT 20
    """).show(truncate=False)

    print("\n-- _new distinct ide_code_completion_model_name --")
    spark.sql(f"""
      SELECT ide_code_completion_model_name, COUNT(*) AS rows,
             COUNT(DISTINCT org_name) AS n_orgs
      FROM {CATALOG}.base_datasets.{NEW_TABLE}
      WHERE copilot_usage_date BETWEEN DATE '2025-08-16' AND DATE '2026-05-12'
      GROUP BY ide_code_completion_model_name
      ORDER BY rows DESC
      LIMIT 20
    """).show(truncate=False)

    print(f"\n-- Does demo-acme-direct have rows in {NEW_TABLE}? --")
    spark.sql(f"""
      SELECT COUNT(*) AS rows_for_demo_acme_direct
      FROM {CATALOG}.base_datasets.{NEW_TABLE}
      WHERE org_name = 'demo-acme-direct'
    """).show(truncate=False)
else:
    print(f"\n  Table {NEW_TABLE} does NOT exist in playground_prod.")
    print("  That means the running API has the isNewVersion flag OFF and the")
    print("  dashboard data is coming from somewhere unexpected. Check what")
    print("  table the dashboard's API request actually targets.")
