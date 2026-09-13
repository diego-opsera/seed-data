# Phase 0 / step 1 — catalog + schema + table reachability for the DVI batch.
#
# Answers: does playground_prod contain every schema and table the VisualForge
# DVI ETL reads, and which ones are missing outright? Missing tables need DDL in
# Phase 2; present-but-empty tables are diagnosed by diag_sources.py.
#
# READ-ONLY. No INSERT / UPDATE / DELETE / CREATE.
#
# Run:
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/diag_catalog.py").read())

CATALOG = "playground_prod"

# Every table syncDvi.js + its shared loaders touch, grouped by schema.
# Sourced from visualforge@b8377ed22 modules/integration/databricks/.
DVI_TABLES = {
    "source_to_stage": [
        "raw_github_pull_requests_rest_api_prs",    # Velocity / Throughput (primary, merged-only)
        "raw_github_pull_requests_rest_api_prs_details",
        "raw_github_commits_rest_api",              # per-dev throughput
        "raw_sonar_metric_split_data_branchwise",   # Security tier 2 + monthly series
        "raw_sonar_type_data_branchwise",           # per-dev Sonar author summary
        "github_actions_runs_rest_api",             # Security tier 3 + deploys
        "github_action_jobs_rest_api",
        "raw_mongo_pipelineactivities",             # preferred deploy source (Impact)
        "raw_mongo_pipelines",
        "raw_github_teams_members",                 # Team Split labels
        "raw_jira_issues_rest_api",              # OPTIONAL — probe fails soft, view is the fallback
    ],
    "base_datasets": [
        "pull_requests",                            # PR fallback (we already seed this)
        "commits_rest_api",                         # commit fallback (we already seed this)
        "v_itsm_issues_hist",                       # Quality + Impact
        "asp_sonar_issues",                         # Security tier 1
        "v_github_teams_members_current",           # Team Split labels
        "v_github_copilot_seats_billing",
    ],
    "consumption_layer": [
        "commits_prs",                              # PR supplement (we already seed this)
        "v_ghas_overview",                       # OPTIONAL — only the vulnerability-mttr metric
        "ai_assistant_license_info",
    ],
    "master_data": [
        "date_dim",                                 # join spine for every time series
        "v_date_dim",
        "team_member_list",                      # OPTIONAL — Workday hierarchy path only
    ],
}


def hr(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


hr("1. Session catalog / schema")
try:
    import json as _json
    _r = spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema").collect()
    print(_json.dumps([r.asDict() for r in _r], default=str, indent=2))
except Exception as e:
    print(f"query failed: {str(e).splitlines()[0][:200]}")

print("NOTE: the above is THIS NOTEBOOK's session, not what the VisualForge backend resolves to.")
print("      VisualForge resolves a catalog per-user via the Opsera scope handler, so confirm the")
print("      app side separately with:  GET /api/v1/databricks/verify-catalog")
print("      (or DATABRICKS_FORCE_CATALOG / DATABRICKS_CATALOG in unified-backend/.env).")
print("      If the app does not resolve to playground_prod, the whole DVI plan retargets.")

hr(f"2. Schemas present in {CATALOG}")
try:
    schemas = {r[0] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()}
    for s in sorted(DVI_TABLES):
        print(f"  {'OK     ' if s in schemas else 'MISSING'}  {s}")
    extra = len(schemas) - len([s for s in DVI_TABLES if s in schemas])
    print(f"\n  ({len(schemas)} schemas total in {CATALOG})")
except Exception as e:
    print(f"SHOW SCHEMAS failed: {str(e).splitlines()[0][:200]}")
    schemas = set()

hr("3. Table reachability")
missing, present = [], []
for schema, tables in DVI_TABLES.items():
    if schema not in schemas:
        print(f"\n-- {schema} -- SCHEMA MISSING, skipping {len(tables)} tables")
        missing.extend(f"{schema}.{t}" for t in tables)
        continue
    try:
        have = {r[1] for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()}
    except Exception as e:
        print(f"\n-- {schema} -- SHOW TABLES failed: {str(e).splitlines()[0][:160]}")
        continue
    print(f"\n-- {schema} --")
    for t in tables:
        if t in have:
            present.append(f"{schema}.{t}")
            print(f"  OK       {t}")
        else:
            missing.append(f"{schema}.{t}")
            print(f"  MISSING  {t}")

hr("4. Verdict")
print(f"  present: {len(present)}")
print(f"  missing: {len(missing)}")
if missing:
    print("\n  Missing:")
    for m in missing:
        print(f"    - {m}")
    print("\n  Tables marked OPTIONAL above do NOT block the five DVI dimension tiles — every one")
    print("  of them is read through safeQuery/a probe and degrades to a fallback. Only create a")
    print("  missing table if diag_dimensions.py shows the dimension it feeds is the one that is dark.")
print("\n  Next: diag_sources.py (row counts + window coverage for the present tables)")
