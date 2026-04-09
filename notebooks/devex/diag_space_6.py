# Diagnostic: simulate commits_vs_space with the actual ROW_NUMBER deduplication
# This shows exactly what the chart SQL returns after deduplication
# Run: exec(open("/tmp/seed-data/notebooks/devex/diag_space_6.py").read())

import json
from datetime import date, timedelta

CATALOG  = "playground_prod"
FGVF     = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"
VIEW     = f"{CATALOG}.source_to_stage.v_survey_details_with_responses"
COMMITS  = f"{CATALOG}.base_datasets.commits_rest_api"
UUID     = "space_d4e5f6g7-h8i9-0123-defg-456789012345"

# "Last 270 days" from today
TO_DATE   = date.today().isoformat()
FROM_DATE = (date.today() - timedelta(days=270)).isoformat()
DD        = 270

def rows(q, limit=30):
    return [r.asDict() for r in spark.sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

print(f"Date range: {FROM_DATE} to {TO_DATE}")

# Full simulation with ROW_NUMBER deduplication (exactly as the real SQL)
out("commits_vs_space.exact_sql_output", rows(f"""
WITH
project_mapping AS (
  SELECT DISTINCT level_1, level_2, level_3, level_4,
         exploded_project_url AS project_url
  FROM {FGVF} fg
  LATERAL VIEW explode_outer(fg.project_url) AS exploded_project_url
  WHERE kpi_uuids = '{UUID}'
    AND level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
    AND exploded_project_url IS NOT NULL
),

survey_scores AS (
  SELECT s.survey_id,
         exploded_level_value AS level_filter,
         AVG(CASE WHEN TRY_CAST(s.answer AS INT) BETWEEN 1 AND 5
                  THEN (TRY_CAST(s.answer AS INT)-1)*25 ELSE 0 END) AS overall_space_score
  FROM {VIEW} s
  LATERAL VIEW explode(s.level_value) AS exploded_level_value
  WHERE level_name = 'level_3'
    AND arrays_overlap(level_value, array('demo-acme-corp'))
    AND survey_id LIKE 'demo-seed-space-%'
    AND DATE(last_submitted_timestamp) BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
  GROUP BY s.survey_id, exploded_level_value
),

survey_project AS (
  SELECT DISTINCT ss.survey_id, ss.overall_space_score, pm.project_url
  FROM survey_scores ss
  JOIN project_mapping pm ON (ss.level_filter = pm.level_1
                            OR ss.level_filter = pm.level_2
                            OR ss.level_filter = pm.level_3
                            OR ss.level_filter = pm.level_4)
  WHERE ss.overall_space_score IS NOT NULL AND pm.project_url IS NOT NULL
),

deduped AS (
  SELECT project_url, survey_id, overall_space_score,
         ROW_NUMBER() OVER (PARTITION BY project_url
                            ORDER BY overall_space_score DESC, survey_id DESC) AS rn
  FROM survey_project
),

unique_mapping AS (SELECT project_url, survey_id, overall_space_score FROM deduped WHERE rn = 1),

commit_metrics AS (
  SELECT project_url,
         COUNT(DISTINCT commit_id) AS total_commits,
         COUNT(DISTINCT commit_date) AS active_days,
         ROUND(TRY_DIVIDE(COUNT(commit_id) * 1.0, {DD}), 1) AS base_commits_per_day
  FROM {COMMITS}
  WHERE commit_date BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
    AND project_url IN (SELECT project_url FROM project_mapping)
    AND SIZE(SPLIT(before_sha, ',')) = 1
  GROUP BY project_url
),

velocity AS (
  SELECT um.survey_id, um.overall_space_score, cm.project_url,
         cm.total_commits, cm.active_days, cm.base_commits_per_day,
         ROUND((cm.total_commits * 1.0 / GREATEST(cm.active_days,1)) * (um.overall_space_score/100.0), 2) AS commit_velocity
  FROM unique_mapping um
  JOIN commit_metrics cm ON um.project_url = cm.project_url
  WHERE cm.total_commits > 0
)

SELECT ROUND(overall_space_score, 2) AS space_score_percentage,
       ROUND(AVG(commit_velocity), 2) AS commits_per_day,
       SUM(base_commits_per_day) AS base_commits_per_day
FROM velocity
GROUP BY survey_id, overall_space_score
ORDER BY overall_space_score
"""))
