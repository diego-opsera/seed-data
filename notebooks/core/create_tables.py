TARGET_CATALOG = "opsera_test"  # change to your playground catalog
TARGET_SCHEMA  = "base_datasets"

ddls = {
"enterprise_level_copilot_metrics": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.enterprise_level_copilot_metrics (
  enterprise_id INT,
  enterprise STRING,
  created_at TIMESTAMP,
  usage_date DATE,
  code_acceptance_activity_count BIGINT,
  code_generation_activity_count BIGINT,
  daily_active_users BIGINT,
  loc_added_sum BIGINT,
  loc_deleted_sum BIGINT,
  loc_suggested_to_add_sum BIGINT,
  loc_suggested_to_delete_sum BIGINT,
  monthly_active_agent_users BIGINT,
  monthly_active_chat_users BIGINT,
  monthly_active_users BIGINT,
  totals_by_feature ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, feature: STRING, generated_loc_sum: BIGINT, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT, user_initiated_interaction_count: BIGINT>>,
  totals_by_ide ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, generated_loc_sum: BIGINT, ide: STRING, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT, user_initiated_interaction_count: BIGINT>>,
  totals_by_language_feature ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, feature: STRING, generated_loc_sum: BIGINT, language: STRING, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT>>,
  totals_by_language_model ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, generated_loc_sum: BIGINT, language: STRING, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT, model: STRING>>,
  totals_by_model_feature ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, feature: STRING, generated_loc_sum: BIGINT, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT, model: STRING, user_initiated_interaction_count: BIGINT>>,
  user_initiated_interaction_count BIGINT,
  weekly_active_users BIGINT)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

"enterprise_user_feature_level_copilot_metrics": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.enterprise_user_feature_level_copilot_metrics (
  usage_date DATE,
  enterprise_id INT,
  enterprise STRING,
  user_id INT,
  user_login STRING,
  assignee_login STRING,
  feature STRING,
  user_initiated_interaction_count BIGINT,
  accepted_loc_sum BIGINT,
  generated_loc_sum BIGINT,
  code_acceptance_activity_count BIGINT,
  code_generation_activity_count BIGINT,
  loc_added_sum BIGINT,
  loc_deleted_sum BIGINT,
  loc_suggested_to_add_sum BIGINT,
  loc_suggested_to_delete_sum BIGINT)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

"enterprise_user_ide_level_copilot_metrics": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.enterprise_user_ide_level_copilot_metrics (
  usage_date DATE,
  enterprise_id INT,
  enterprise STRING,
  user_id INT,
  user_login STRING,
  assignee_login STRING,
  ide STRING,
  user_initiated_interaction_count BIGINT,
  accepted_loc_sum BIGINT,
  generated_loc_sum BIGINT,
  code_acceptance_activity_count BIGINT,
  code_generation_activity_count BIGINT,
  loc_added_sum BIGINT,
  loc_deleted_sum BIGINT,
  loc_suggested_to_add_sum BIGINT,
  loc_suggested_to_delete_sum BIGINT,
  last_known_plugin_version STRUCT<plugin: STRING, plugin_version: STRING, sampled_at: STRING>)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

"enterprise_user_language_model_level_copilot_metrics": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.enterprise_user_language_model_level_copilot_metrics (
  usage_date DATE,
  enterprise_id INT,
  enterprise STRING,
  user_id INT,
  user_login STRING,
  assignee_login STRING,
  language STRING,
  model STRING,
  accepted_loc_sum BIGINT,
  generated_loc_sum BIGINT,
  code_acceptance_activity_count BIGINT,
  code_generation_activity_count BIGINT,
  loc_added_sum BIGINT,
  loc_deleted_sum BIGINT,
  loc_suggested_to_add_sum BIGINT,
  loc_suggested_to_delete_sum BIGINT)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",

"enterprise_user_level_copilot_metrics": f"""
CREATE TABLE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}.enterprise_user_level_copilot_metrics (
  usage_date DATE,
  enterprise_id INT,
  enterprise STRING,
  user_id INT,
  user_login STRING,
  assignee_login STRING,
  user_initiated_interaction_count BIGINT,
  code_generation_activity_count BIGINT,
  code_acceptance_activity_count BIGINT,
  used_agent BOOLEAN,
  used_chat BOOLEAN,
  loc_suggested_to_add_sum DOUBLE,
  loc_suggested_to_delete_sum DOUBLE,
  loc_added_sum DOUBLE,
  loc_deleted_sum DOUBLE,
  totals_by_ide ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, generated_loc_sum: BIGINT, ide: STRING, last_known_plugin_version: STRUCT<plugin: STRING, plugin_version: STRING, sampled_at: STRING>, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT, user_initiated_interaction_count: BIGINT>>,
  totals_by_feature ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, feature: STRING, generated_loc_sum: BIGINT, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT, user_initiated_interaction_count: BIGINT>>,
  totals_by_language_feature ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, feature: STRING, generated_loc_sum: BIGINT, language: STRING, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT>>,
  totals_by_language_model ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, generated_loc_sum: BIGINT, language: STRING, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT, model: STRING>>,
  totals_by_model_feature ARRAY<STRUCT<accepted_loc_sum: BIGINT, code_acceptance_activity_count: BIGINT, code_generation_activity_count: BIGINT, feature: STRING, generated_loc_sum: BIGINT, loc_added_sum: BIGINT, loc_deleted_sum: BIGINT, loc_suggested_to_add_sum: BIGINT, loc_suggested_to_delete_sum: BIGINT, model: STRING, user_initiated_interaction_count: BIGINT>>)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""",
}

for name, ddl in ddls.items():
    print(f"Creating {name}...", end=" ")
    spark.sql(ddl)
    print("done")

print("\nVerifying...")
for name in ddls:
    count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.{name}").collect()[0][0]
    print(f"  {name}: {count} rows")
