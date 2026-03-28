-- Restore script for v_github_copilot_seats_usage_user_level
-- Run this in a Databricks SQL cell if you need to recreate the view
-- after converting it to a table for seed data testing.
--
-- Pre-requisite: DROP TABLE playground_prod.base_datasets.v_github_copilot_seats_usage_user_level

CREATE VIEW playground_prod.base_datasets.v_github_copilot_seats_usage_user_level (
  copilot_usage_datetime,
  copilot_usage_date,
  copilot_usage_hour,
  org_name,
  assigning_team_id,
  assigning_team_name,
  org_assignee_login,
  cleansed_org_assignee_login,
  assignee_id,
  assignee_login,
  cleansed_assignee_login,
  last_activity_editor,
  created_at,
  updated_at,
  pending_cancellation_date,
  record_insert_datetime)
WITH SCHEMA COMPENSATION
AS SELECT
  cast(a.last_activity_at as timestamp)                                                                                                    AS copilot_usage_datetime,
  cast(a.last_activity_at as date)                                                                                                         AS copilot_usage_date,
  HOUR(cast(a.last_activity_at as timestamp))                                                                                              AS copilot_usage_hour,
  lv.org_name,
  a.assigning_team_id,
  a.assigning_team_name,
  a.org_assignee_login,
  CASE WHEN lv.org_name = 'cisco-sbg' THEN regexp_replace(trim(lower(a.org_assignee_login)), 'cisco|_|-|@', '') ELSE a.org_assignee_login END AS cleansed_org_assignee_login,
  a.assignee_id,
  a.assignee_login,
  CASE WHEN lv.org_name = 'cisco-sbg' THEN a.org_assignee_login ELSE a.assignee_login END                                                 AS cleansed_assignee_login,
  a.last_activity_editor,
  cast(a.created_at as timestamp)                                                                                                          AS created_at,
  cast(a.updated_at as timestamp)                                                                                                          AS updated_at,
  cast(a.pending_cancellation_date as date)                                                                                                AS pending_cancellation_date,
  cast(a.source_record_insert_datetime as timestamp)                                                                                       AS record_insert_datetime
FROM source_to_stage.raw_github_copilot_seats a
JOIN master_data.github_copilot_orgs_mapping b ON a.org_name = b.org_name
LATERAL VIEW explode(linked_org_name) lv AS org_name;
