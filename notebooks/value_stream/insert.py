# Seeds playground_prod.user_working.offerings_jira_pipeline_details for both
# Acme + Meridian to power the Issue Stream / Flow View feature in
# vnxt-insights-api (src/queries/value-stream/*.sql).
#
# Pre-req: notebooks/value_stream/create_table.py must have been run once.
# Date window: rolling 1-year, comes from narrative.yaml (rewritten daily by
# notebooks/insert.py).

import sys, os
from datetime import date

# Module cache-bust — same pattern as direct/, dora/, meridian/ insert scripts
for _key in list(sys.modules.keys()):
    if _key.startswith("generators"):
        del sys.modules[_key]

sys.path.insert(0, "/tmp/seed-data")
os.chdir("/tmp/seed-data")

from generators import value_stream, pipeline_failures
from generators.utils import load_story

CATALOG = "playground_prod"

# Story dates are computed at runtime — rolling 1-year window ending today.
# pipeline-failures.sql hardcodes `pipeline_started_at >= DATE_SUB(CURRENT_DATE(),
# 30)` so seeded rows must align with the cluster's real "today".
narrative = load_story("narrative")

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

# ── Part 2: Pipeline Failures bridge ──────────────────────────────────────────
# Adds failure rows to base_datasets.pipeline_activities (scoped by
# record_inserted_by = 'seed-data-value-stream' so it won't touch dora's data)
# plus matching rows in repo_pipeline_details + github_offering_workflow_job_logs.
#
# `today` is the reference for the rolling 30-day failure window AND the
# anchor for the "today's narrative incident" cluster. We use the
# narrative end_date (just rewritten to date.today() above) so failure
# dates line up exactly with the seeded ticket dates.

today = date.fromisoformat(narrative["end_date"])

for cfg, label in [(value_stream.ACME, "Acme"), (value_stream.MERIDIAN, "Meridian")]:
    bundles = pipeline_failures.generate(CATALOG, cfg, narrative, today)
    for table_label, statements in bundles.items():
        print(f"\n[Pipeline Failures - {label}] {table_label}: {len(statements)} INSERT batch(es)")
        for i, sql in enumerate(statements, 1):
            print(f"  [{i}/{len(statements)}] inserting...", end=" ")
            spark.sql(sql)
            print("done")

# Verify failure row counts (must all match — they're inserted as triples)
n_pa = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE record_inserted_by = 'seed-data-value-stream'
""").collect()[0]["n"]
n_rpd = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.user_working.repo_pipeline_details
    WHERE record_inserted_by = 'seed-data-value-stream'
""").collect()[0]["n"]
n_logs = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.user_working.github_offering_workflow_job_logs
    WHERE record_inserted_by = 'seed-data-value-stream'
""").collect()[0]["n"]
print(f"\nFailure rows: pipeline_activities={n_pa}, repo_pipeline_details={n_rpd}, logs={n_logs}")
