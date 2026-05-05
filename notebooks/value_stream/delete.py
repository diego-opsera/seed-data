CATALOG = "playground_prod"

# All deletes are tightly scoped — none touch dora's pipeline_activities rows
# (those are tagged record_inserted_by = 'seed-data' / 'seed-data-meridian').
# Our failure rows use 'seed-data-value-stream' as a distinct scope tag.

_deletes = [
    # Main fact table — scoped to demo orgs
    ("user_working.offerings_jira_pipeline_details",
     "org_name IN ('demo-acme-direct', 'demo-meridian')"),
    # Pipeline Failures bridge tables — scoped to our distinct record_inserted_by tag
    ("base_datasets.pipeline_activities",
     "record_inserted_by = 'seed-data-value-stream'"),
    ("user_working.repo_pipeline_details",
     "record_inserted_by = 'seed-data-value-stream'"),
    ("user_working.github_offering_workflow_job_logs",
     "record_inserted_by = 'seed-data-value-stream'"),
]

for table, predicate in _deletes:
    fqn = f"{CATALOG}.{table}"
    try:
        n = spark.sql(f"SELECT COUNT(*) FROM {fqn} WHERE {predicate}").collect()[0][0]
        spark.sql(f"DELETE FROM {fqn} WHERE {predicate}")
        print(f"{table}: deleted {n} rows")
    except Exception as e:
        print(f"{table}: ERROR — {e}")
