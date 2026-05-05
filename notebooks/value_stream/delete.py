CATALOG = "playground_prod"

# Scoped strictly to demo orgs. The user_working schema also contains 4 other
# unrelated tables (copilot_licenses_usage_metric_view, pull_requests_bkup_5june,
# raw_github_copilot_28_days_user_metrics_temp_*) — those are NOT touched by this script.

table     = "user_working.offerings_jira_pipeline_details"
fqn       = f"{CATALOG}.{table}"
predicate = "org_name IN ('demo-acme-direct', 'demo-meridian')"

try:
    n = spark.sql(f"SELECT COUNT(*) FROM {fqn} WHERE {predicate}").collect()[0][0]
    spark.sql(f"DELETE FROM {fqn} WHERE {predicate}")
    print(f"{table}: deleted {n} rows")
except Exception as e:
    print(f"{table}: ERROR — {e}")
