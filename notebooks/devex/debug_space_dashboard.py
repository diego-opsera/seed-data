# SPACE dashboard — diagnose "Cannot read properties of undefined (reading 'toFixed')"
#
# Runs each SPACE chart's actual SQL end-to-end against the seed and shows
# the result shape. Any chart returning 0 rows, or any row with a NULL
# numeric field, is a candidate for the frontend toFixed() crash.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/devex/debug_space_dashboard.py").read())

from datetime import date, timedelta

CATALOG = "playground_prod"

# Dashboard date range — try the one that's crashing the UI.
# Adjust these to match what's selected in the dashboard filter.
TO_DATE   = date.today().isoformat()
FROM_DATE = (date.today() - timedelta(days=30)).isoformat()

# Filter level — matches the dashboard's "Project: Acme Corp" filter
# (level_1 has priority in getSpaceDashboardFilterClause)
SPACE_FILTER = "WHERE level_name = 'level_1' AND arrays_overlap(level_value, array('Acme Corp'))"

print()
print("=" * 78)
print(f"date range: {FROM_DATE} → {TO_DATE}")
print(f"space filter: {SPACE_FILTER}")
print("=" * 78)

# --- 1. Raw survey rows in the view (does our data even appear?) -----------
print("\n[1] Survey rows surfaced by the view for the current period")
spark.sql(f"""
  SELECT COUNT(*) AS n_rows,
         COUNT(DISTINCT survey_id) AS n_surveys,
         COUNT(DISTINCT response_id) AS n_responses,
         COUNT(DISTINCT question_id) AS n_questions,
         MIN(date(last_submitted_timestamp)) AS min_date,
         MAX(date(last_submitted_timestamp)) AS max_date
  FROM {CATALOG}.source_to_stage.v_survey_details_with_responses
  {SPACE_FILTER}
    AND date(last_submitted_timestamp) BETWEEN DATE '{FROM_DATE}' AND DATE '{TO_DATE}'
""").show(truncate=False)

print("\n[2] Same for the previous period (current_period - dd days)")
spark.sql(f"""
  WITH v AS (
    SELECT TO_DATE('{FROM_DATE}') AS s, TO_DATE('{TO_DATE}') AS e,
           DATE_DIFF(day, TO_DATE('{FROM_DATE}'), TO_DATE('{TO_DATE}')) + 1 AS dd
  )
  SELECT COUNT(*) AS n_rows,
         COUNT(DISTINCT survey_id) AS n_surveys
  FROM {CATALOG}.source_to_stage.v_survey_details_with_responses, v
  {SPACE_FILTER}
    AND date(last_submitted_timestamp) BETWEEN DATE_SUB(v.s, CAST(v.dd AS INT)) AND DATE_SUB(v.s, 1)
""").show(truncate=False)

# --- 3. space_overview.sql (simplified) -------------------------------------
print("\n[3] space_overview output (returns one row per period — every field should be non-NULL)")
spark.sql(f"""
  WITH variables AS (
    SELECT TO_DATE('{FROM_DATE}') AS start_date, TO_DATE('{TO_DATE}') AS end_date,
           DATE_DIFF(day, TO_DATE('{FROM_DATE}'), TO_DATE('{TO_DATE}')) + 1 AS dd,
           DATE_SUB(TO_DATE('{FROM_DATE}'), CAST(DATE_DIFF(day, TO_DATE('{FROM_DATE}'), TO_DATE('{TO_DATE}')) + 1 AS INT)) AS start_date1,
           DATE_SUB(TO_DATE('{FROM_DATE}'), 1) AS end_date1
  ),
  space_dimensions AS (
    SELECT 's' AS dimension, question_id FROM (VALUES ('257bb6de'), ('09ebec35')) AS t(question_id)
    UNION ALL SELECT 'p', question_id FROM (VALUES ('036cc641'), ('68215ab9'), ('3914480f')) AS t(question_id)
    UNION ALL SELECT 'a', question_id FROM (VALUES ('14d4f094'), ('12366098')) AS t(question_id)
    UNION ALL SELECT 'c', question_id FROM (VALUES ('54e3ea5f'), ('3755645e')) AS t(question_id)
    UNION ALL SELECT 'e', question_id FROM (VALUES ('04a3d0c5'), ('300e51ec')) AS t(question_id)
  ),
  survey_responses AS (
    SELECT survey_id, response_id, question_id, last_submitted_timestamp,
      CASE
        WHEN TO_DATE(last_submitted_timestamp) >= (SELECT start_date FROM variables)
             AND TO_DATE(last_submitted_timestamp) <= (SELECT end_date FROM variables) THEN 'current_period'
        WHEN TO_DATE(last_submitted_timestamp) >= (SELECT start_date1 FROM variables)
             AND TO_DATE(last_submitted_timestamp) <= (SELECT end_date1 FROM variables) THEN 'previous_period'
      END AS time_period,
      CASE
        WHEN answer = 'Yes' THEN 100 WHEN answer = 'No' THEN 0
        WHEN TRY_CAST(answer AS INT) BETWEEN 1 AND 5 THEN (TRY_CAST(answer AS INT) - 1) * 25
        ELSE 0
      END AS question_score
    FROM {CATALOG}.source_to_stage.v_survey_details_with_responses
    {SPACE_FILTER}
      AND date(last_submitted_timestamp) BETWEEN (SELECT start_date1 FROM variables) AND (SELECT end_date FROM variables)
      AND question_id IN (SELECT question_id FROM space_dimensions)
  ),
  response_dimension_scores AS (
    SELECT survey_id, response_id, time_period,
      AVG(CASE WHEN sd.dimension = 's' THEN sr.question_score END) AS s_score,
      AVG(CASE WHEN sd.dimension = 'p' THEN sr.question_score END) AS p_score,
      AVG(CASE WHEN sd.dimension = 'a' THEN sr.question_score END) AS a_score,
      AVG(CASE WHEN sd.dimension = 'c' THEN sr.question_score END) AS c_score,
      AVG(CASE WHEN sd.dimension = 'e' THEN sr.question_score END) AS e_score
    FROM survey_responses sr
    JOIN space_dimensions sd ON sr.question_id = sd.question_id
    WHERE sr.time_period IS NOT NULL
    GROUP BY survey_id, response_id, time_period
  )
  SELECT time_period AS period,
    ROUND(AVG(COALESCE(s_score, 0)), 2) AS avg_s_score,
    ROUND(AVG(COALESCE(p_score, 0)), 2) AS avg_p_score,
    ROUND(AVG(COALESCE(a_score, 0)), 2) AS avg_a_score,
    ROUND(AVG(COALESCE(c_score, 0)), 2) AS avg_c_score,
    ROUND(AVG(COALESCE(e_score, 0)), 2) AS avg_e_score,
    ROUND(AVG((COALESCE(s_score,0)+COALESCE(p_score,0)+COALESCE(a_score,0)+COALESCE(c_score,0)+COALESCE(e_score,0))/5), 2) AS overall_space_score,
    COUNT(DISTINCT response_id) AS active_respondents
  FROM response_dimension_scores
  GROUP BY time_period
""").show(truncate=False)

# --- 4. space_dimension_metrics.sql (NO outer COALESCE — can return NULL fields) ---
print("\n[4] space_dimension_metrics output — *** NULL in any score column = crash culprit ***")
spark.sql(f"""
  WITH variables AS (
    SELECT TO_DATE('{FROM_DATE}') AS start_date, TO_DATE('{TO_DATE}') AS end_date,
           DATE_DIFF(day, TO_DATE('{FROM_DATE}'), TO_DATE('{TO_DATE}')) + 1 AS dd,
           DATE_SUB(TO_DATE('{FROM_DATE}'), CAST(DATE_DIFF(day, TO_DATE('{FROM_DATE}'), TO_DATE('{TO_DATE}')) + 1 AS INT)) AS start_date1,
           DATE_SUB(TO_DATE('{FROM_DATE}'), 1) AS end_date1
  ),
  dimension_questions AS (
    SELECT 's' AS dimension, question_id FROM (VALUES ('257bb6de'), ('09ebec35')) AS t(question_id)
    UNION ALL SELECT 'p', question_id FROM (VALUES ('036cc641'), ('68215ab9'), ('3914480f')) AS t(question_id)
    UNION ALL SELECT 'a', question_id FROM (VALUES ('14d4f094'), ('12366098')) AS t(question_id)
    UNION ALL SELECT 'c', question_id FROM (VALUES ('54e3ea5f'), ('3755645e')) AS t(question_id)
    UNION ALL SELECT 'e', question_id FROM (VALUES ('04a3d0c5'), ('300e51ec')) AS t(question_id)
  ),
  survey_responses AS (
    SELECT question_id, response_id, last_submitted_timestamp,
      CASE
        WHEN TO_DATE(last_submitted_timestamp) >= (SELECT start_date FROM variables)
             AND TO_DATE(last_submitted_timestamp) <= (SELECT end_date FROM variables) THEN 'current_period'
        WHEN TO_DATE(last_submitted_timestamp) >= (SELECT start_date1 FROM variables)
             AND TO_DATE(last_submitted_timestamp) <= (SELECT end_date1 FROM variables) THEN 'previous_period'
      END AS time_period,
      CASE
        WHEN answer = 'Yes' THEN 100 WHEN answer = 'No' THEN 0
        WHEN TRY_CAST(answer AS INT) BETWEEN 1 AND 5 THEN (TRY_CAST(answer AS INT) - 1) * 25
        ELSE 0
      END AS question_score
    FROM {CATALOG}.source_to_stage.v_survey_details_with_responses
    {SPACE_FILTER}
      AND date(last_submitted_timestamp) BETWEEN (SELECT start_date FROM variables) AND (SELECT end_date FROM variables)
      AND question_id IN (SELECT question_id FROM dimension_questions)
  ),
  dimension_metrics AS (
    SELECT sr.time_period, dq.dimension,
      SUM(sr.question_score) AS dimension_sum,
      COUNT(DISTINCT sr.question_id) AS question_count,
      COUNT(DISTINCT sr.response_id) AS respondent_count
    FROM survey_responses sr JOIN dimension_questions dq ON sr.question_id = dq.question_id
    WHERE sr.time_period IS NOT NULL
    GROUP BY sr.time_period, dq.dimension
  ),
  dimension_scores AS (
    SELECT time_period, dimension,
      CASE WHEN (question_count*respondent_count*100) > 0
           THEN ROUND((dimension_sum*100.0)/(question_count*respondent_count*100), 2)
           ELSE 0 END AS dimension_score
    FROM dimension_metrics
  )
  SELECT time_period AS period,
    MAX(CASE WHEN dimension='s' THEN dimension_score END) AS s_score,
    MAX(CASE WHEN dimension='p' THEN dimension_score END) AS p_score,
    MAX(CASE WHEN dimension='a' THEN dimension_score END) AS a_score,
    MAX(CASE WHEN dimension='c' THEN dimension_score END) AS c_score,
    MAX(CASE WHEN dimension='e' THEN dimension_score END) AS e_score
  FROM dimension_scores GROUP BY time_period
""").show(truncate=False)

# --- 5. space_devex_metrics — CROSS JOIN current x previous, often empty -----
print("\n[5] space_devex_metrics — empty result = CROSS JOIN found no current+previous overlap")
print("    (depends on base_datasets.commits_rest_api + pull_requests, NOT surveys)")
spark.sql(f"""
  SELECT
    'current ' AS period,
    COUNT(DISTINCT c.commit_id) AS n_commits,
    (SELECT COUNT(*) FROM {CATALOG}.base_datasets.pull_requests p
     WHERE p.org_name = 'demo-acme-direct'
       AND TO_DATE(p.pr_created_datetime) BETWEEN DATE '{FROM_DATE}' AND DATE '{TO_DATE}') AS n_prs
  FROM {CATALOG}.base_datasets.commits_rest_api c
  WHERE c.org_name = 'demo-acme-direct'
    AND c.commit_date BETWEEN DATE '{FROM_DATE}' AND DATE '{TO_DATE}'
  UNION ALL
  SELECT
    'previous',
    (SELECT COUNT(DISTINCT c.commit_id) FROM {CATALOG}.base_datasets.commits_rest_api c
     WHERE c.org_name = 'demo-acme-direct'
       AND c.commit_date BETWEEN DATE_SUB(DATE '{FROM_DATE}', DATE_DIFF(day, DATE '{FROM_DATE}', DATE '{TO_DATE}')+1) AND DATE_SUB(DATE '{FROM_DATE}', 1)),
    (SELECT COUNT(*) FROM {CATALOG}.base_datasets.pull_requests p
     WHERE p.org_name = 'demo-acme-direct'
       AND TO_DATE(p.pr_created_datetime) BETWEEN DATE_SUB(DATE '{FROM_DATE}', DATE_DIFF(day, DATE '{FROM_DATE}', DATE '{TO_DATE}')+1) AND DATE_SUB(DATE '{FROM_DATE}', 1))
""").show(truncate=False)

# --- 6. View schema (so we know expected columns) ----------------------------
print("\n[6] v_survey_details_with_responses schema")
spark.sql(f"DESCRIBE {CATALOG}.source_to_stage.v_survey_details_with_responses").show(30, truncate=False)

print()
print("=" * 78)
print("Interpretation")
print("=" * 78)
print("""
[1] empty → spaceDashboardFilterClause finds 0 rows. Either filter level
            mismatch (try level_3 = 'demo-acme-corp' instead of level_1)
            or our seed rows have a malformed `filters` NAMED_STRUCT.

[2] empty → previous_period is empty → space_devex_metrics CROSS JOIN
            returns 0 rows → frontend crashes.

[3] NULL in any score field → response_dimension_scores didn't include all
            dimensions. Means some dimension's questions weren't answered
            in any survey within the date range. (Shouldn't happen with
            our seed — all 11 q's per respondent.)

[4] NULL in s/p/a/c/e columns → this query's outputs feed the frontend
            without COALESCE. NULL here is the most likely toFixed crash.

[5] '0' for current or previous → space_devex_metrics finds no commits/PRs
            in that period → CROSS JOIN empty → frontend crashes.

[6] Check that level_name and level_value columns exist as expected.
""")
