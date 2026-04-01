CATALOG  = "playground_prod"
TEST_ORG = "demo-acme-direct"

# ── Scoped deletes ────────────────────────────────────────────────────────────
# Every delete below is tightly scoped so OpseraEngineering (and all other orgs)
# are never touched:
#   - org_name tables       : WHERE org_name      = 'demo-acme-direct'
#   - organization tables   : WHERE organization  = 'demo-acme-direct'
#   - pull_requests         : WHERE merge_request_id LIKE 'demo-seed-pr-%'
#   - file_extensions       : no-op (MERGE insert, nothing to remove)
# ─────────────────────────────────────────────────────────────────────────────

# Tables whose org column is named org_name
org_name_tables = [
    ("base_datasets",   "trf_github_copilot_direct_data"),
    ("base_datasets",   "commits_rest_api"),
    ("source_to_stage", "raw_github_copilot_seats"),
    ("master_data",     "github_copilot_orgs_mapping"),
    ("base_datasets",   "github_copilot_metrics_ide_org_level"),
]

# Tables whose org column is named organization
organization_tables = [
    ("base_datasets",   "code_scan_alert"),
    ("base_datasets",   "secret_scan_alert"),
]

for schema, table in org_name_tables:
    n = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

for schema, table in organization_tables:
    n = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE organization = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

# pull_requests: scoped to our seeded IDs only — never touches OpseraEngineering rows
n = spark.sql(
    f"DELETE FROM {CATALOG}.base_datasets.pull_requests "
    f"WHERE merge_request_id LIKE 'demo-seed-pr-%'"
).collect()[0][0]
print(f"Deleted {n} rows from base_datasets.pull_requests")

# file_extensions: shared reference table, insert is MERGE (no-op if row exists)
print("Skipped master_data.file_extensions (shared reference table — MERGE insert only)")

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
n = spark.sql(
    f"SELECT COUNT(*) FROM {CATALOG}.base_datasets.pull_requests "
    f"WHERE merge_request_id LIKE 'demo-seed-pr-%'"
).collect()[0][0]
print(f"  base_datasets.pull_requests (demo-seed-pr-* only): {n}")
print(f"  master_data.file_extensions: (not cleaned — shared reference table)")
