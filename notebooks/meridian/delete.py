CATALOG = "playground_prod"

# All Meridian rows are isolated by distinct identifiers — no Acme data will be touched.
#
# DORA tables:
#   record_inserted_by = 'seed-data-meridian'   (pipeline_activities, pipeline_deployment_commits, cfr_mttr_metric_data)
#   customer_id = 'demo-meridian'               (mt_itsm_issues_hist/current)
#
# Direct / Copilot tables:
#   org_name = 'demo-meridian'                  (trf_github_copilot_direct_data, raw_github_copilot_seats,
#                                                raw_github_copilot_billing, github_copilot_orgs_mapping,
#                                                github_copilot_metrics_ide_org_level,
#                                                ai_code_assistant_usage_user_level)
#   organization = 'demo-meridian'              (code_scan_alert, secret_scan_alert)
#   level_name = 'demo-meridian'                (ai_assistant_acceptance_info)
#
# Filter config:
#   createdBy / created_by = 'seed-data-meridian@demo.io'  (filter_groups_unity, filter_values_unity)
#   org_name = 'demo-meridian'                             (raw_jira_boards_ci)

_deletes = [
    # DORA base tables
    ("base_datasets.pipeline_activities",
     "record_inserted_by = 'seed-data-meridian'"),
    ("base_datasets.pipeline_deployment_commits",
     "record_inserted_by = 'seed-data-meridian'"),
    ("base_datasets.cfr_mttr_metric_data",
     "record_inserted_by = 'seed-data-meridian'"),
    # ITSM (CTFC)
    ("transform_stage.mt_itsm_issues_hist",
     "customer_id = 'demo-meridian'"),
    ("transform_stage.mt_itsm_issues_current",
     "customer_id = 'demo-meridian'"),
    # Direct / Copilot
    ("base_datasets.trf_github_copilot_direct_data",
     "org_name = 'demo-meridian'"),
    ("source_to_stage.raw_github_copilot_seats",
     "org_name = 'demo-meridian'"),
    ("source_to_stage.raw_github_copilot_billing",
     "org_name = 'demo-meridian'"),
    ("master_data.github_copilot_orgs_mapping",
     "org_name = 'demo-meridian'"),
    ("base_datasets.github_copilot_metrics_ide_org_level",
     "org_name = 'demo-meridian'"),
    ("base_datasets.code_scan_alert",
     "organization = 'demo-meridian'"),
    ("base_datasets.secret_scan_alert",
     "organization = 'demo-meridian'"),
    ("consumption_layer.ai_code_assistant_usage_user_level",
     "org_name = 'demo-meridian'"),
    ("consumption_layer.ai_assistant_acceptance_info",
     "level_name = 'demo-meridian'"),
    # DevEx
    ("base_datasets.commits_rest_api",
     "org_name = 'demo-meridian'"),
    ("base_datasets.pull_requests",
     "merge_request_id LIKE 'meridian-seed-pr-%'"),
    # Filter config
    ("master_data.filter_values_unity",
     "created_by = 'seed-data-meridian@demo.io'"),
    ("master_data.filter_groups_unity",
     "createdBy = 'seed-data-meridian@demo.io'"),
    ("source_to_stage.raw_jira_boards_ci",
     "org_name = 'demo-meridian'"),
]

for table, predicate in _deletes:
    try:
        n = spark.sql(
            f"SELECT COUNT(*) FROM {CATALOG}.{table} WHERE {predicate}"
        ).collect()[0][0]
        spark.sql(f"DELETE FROM {CATALOG}.{table} WHERE {predicate}")
        print(f"{table}: deleted {n} rows")
    except Exception as e:
        print(f"{table}: ERROR — {e}")
