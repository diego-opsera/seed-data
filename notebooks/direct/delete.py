CATALOG  = "playground_prod"
TEST_ORG = "demo-acme-direct"

# Tables scoped by org_name = 'demo-acme-direct'
org_scoped_tables = [
    ("base_datasets",   "trf_github_copilot_direct_data"),
    ("base_datasets",   "commits_rest_api"),
    ("source_to_stage", "raw_github_copilot_seats"),
    ("master_data",     "github_copilot_orgs_mapping"),
    ("base_datasets",   "github_copilot_metrics_ide_org_level"),
    ("base_datasets",   "code_scan_alert"),
    ("base_datasets",   "secret_scan_alert"),
]

for schema, table in org_scoped_tables:
    result = spark.sql(
        f"DELETE FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    )
    n = result.collect()[0][0]
    print(f"Deleted {n} rows from {schema}.{table}")

# Reference table — delete only the extensions we inserted, not the full table
_OUR_EXTENSIONS = (
    "'ts','tsx','py','go','cs','ex','exs','js','jsx',"
    "'java','rb','rs','cpp','kt','swift','php','scala','sh','vue'"
)
result = spark.sql(
    f"DELETE FROM {CATALOG}.master_data.file_extensions "
    f"WHERE code_file_extension IN ({_OUR_EXTENSIONS})"
)
print(f"Deleted {result.collect()[0][0]} rows from master_data.file_extensions")

print("\nVerifying (should all be 0):")
for schema, table in org_scoped_tables:
    n = spark.sql(
        f"SELECT COUNT(*) FROM {CATALOG}.{schema}.{table} WHERE org_name = '{TEST_ORG}'"
    ).collect()[0][0]
    print(f"  {schema}.{table}: {n}")
n = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.master_data.file_extensions").collect()[0][0]
print(f"  master_data.file_extensions: {n}")
