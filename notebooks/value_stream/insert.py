# Seeds playground_prod.user_working.offerings_jira_pipeline_details for both
# Acme + Meridian to power the Issue Stream / Flow View feature in
# vnxt-insights-api (src/queries/value-stream/*.sql).
#
# Pre-req: notebooks/value_stream/create_table.py must have been run once.
# Date window: rolling 1-year, comes from narrative.yaml (rewritten daily by
# notebooks/insert.py).

import sys, os, yaml

# Module cache-bust — same pattern as direct/, dora/, meridian/ insert scripts
for _key in list(sys.modules.keys()):
    if _key.startswith("generators"):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

from generators import value_stream

CATALOG = "playground_prod"

narrative = yaml.safe_load(open("config/stories/narrative.yaml"))

for cfg, label in [(value_stream.ACME, "Acme"), (value_stream.MERIDIAN, "Meridian")]:
    statements = value_stream.generate(CATALOG, cfg, narrative)
    print(f"\n[{label}] {cfg.ticket_count} tickets → {len(statements)} INSERT batches")
    for i, sql in enumerate(statements, 1):
        print(f"  [{i}/{len(statements)}] inserting batch...", end=" ")
        spark.sql(sql)
        print("done")

# Verify
n = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {CATALOG}.user_working.offerings_jira_pipeline_details
    WHERE org_name IN ('demo-acme-direct', 'demo-meridian')
""").collect()[0]["n"]
print(f"\nofferings_jira_pipeline_details: {n} rows for demo orgs")

# Spot-check distinct hierarchy values surfaced to the filter dropdowns
spark.sql(f"""
    SELECT DISTINCT sbg, gbe, offering, jira_project
    FROM {CATALOG}.user_working.offerings_jira_pipeline_details
    WHERE org_name IN ('demo-acme-direct', 'demo-meridian')
    ORDER BY sbg, gbe, offering
""").show(truncate=False)
