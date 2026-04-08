CATALOG  = "playground_prod"
TEST_ORG = "demo-acme-direct"

# ── Scoped deletes ────────────────────────────────────────────────────────────
# Every delete below is tightly scoped so OpseraEngineering (and all other orgs)
# are never touched:
#   - org_name tables       : WHERE org_name      = 'demo-acme-direct'
#   - organization tables   : WHERE organization  = 'demo-acme-direct'
#   - file_extensions       : no-op (MERGE insert, nothing to remove)
#
# NOT handled here (owned by devex/delete.py):
#   pull_requests, commits_rest_api, raw_github_teams_members,
#   mt_itsm_issues_current, mt_itsm_issues_hist
# ─────────────────────────────────────────────────────────────────────────────

# source_to_stage tables with org_name scope
source_to_stage_tables = []

# Tables whose org column is named org_name
org_name_tables = [
    ("base_datasets",   "trf_github_copilot_direct_data"),
    ("source_to_stage", "raw_github_copilot_seats"),
    ("source_to_stage", "raw_github_copilot_billing"),
    ("master_data",     "github_copilot_orgs_mapping"),
    ("base_datasets",   "github_copilot_metrics_ide_org_level"),
]

# Tables whose org column is named organization
organization_tables = [
    ("base_datasets",   "code_scan_alert"),
    ("base_datasets",   "secret_scan_alert"),
]

# consumption_layer tables
consumption_org_name_tables = [
    ("consumption_layer", "ai_code_assistant_usage_user_level"),
]

consumption_level_name_tables = [
    ("consumption_layer", "ai_assistant_acceptance_info"),
]

itsm_tables = []  # owned by devex/delete.py

for schema, table in source_to_stage_tables + org_name_tables:
    n = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

for schema, table in organization_tables:
    n = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE organization = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

for schema, table in consumption_org_name_tables:
    n = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

for schema, table in consumption_level_name_tables:
    n = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE level_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

for schema, table in itsm_tables:
    n = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE customer_id = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

# file_extensions: shared reference table, insert is MERGE (no-op if row exists)
print("Skipped master_data.file_extensions (shared reference table — MERGE insert only)")
# pull_requests / commits / teams / itsm: owned by devex/delete.py (not handled here)

# ── Verification ──────────────────────────────────────────────────────────────
print("\nVerifying (should all be 0):")
for schema, table in org_name_tables:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"  {schema}.{table}: {n}")
for schema, table in organization_tables:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table} WHERE organization = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"  {schema}.{table}: {n}")
for schema, table in consumption_org_name_tables:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"  {schema}.{table}: {n}")
for schema, table in consumption_level_name_tables:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table} WHERE level_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"  {schema}.{table}: {n}")
for schema, table in itsm_tables:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table} WHERE customer_id = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"  {schema}.{table}: {n}")
print(f"  master_data.file_extensions: (not cleaned — shared reference table)")
