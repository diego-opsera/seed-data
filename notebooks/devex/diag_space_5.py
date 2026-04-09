# Diagnostic: simulate commits_vs_space join step by step
# Run: exec(open("/tmp/seed-data/notebooks/devex/diag_space_5.py").read())

import json

CATALOG  = "playground_prod"
FGVF     = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"
VIEW     = f"{CATALOG}.source_to_stage.v_survey_details_with_responses"
COMMITS  = f"{CATALOG}.base_datasets.commits_rest_api"
UUID     = "space_d4e5f6g7-h8i9-0123-defg-456789012345"

# Use a broad date range covering our full story
FROM_DATE = "2025-01-01"
TO_DATE   = "2026-04-09"

def rows(q, limit=10):
    return [r.asDict() for r in spark.sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

# 1. Does commits_rest_api have a commit_author column?
out("commits.schema_sample", rows(f"""
    SELECT * FROM {COMMITS} WHERE org_name = 'demo-acme-direct' LIMIT 1
"""))

# 2. Simulate commit_data CTE from commits_vs_space (using commit_author if it exists,
#    or cleansed_commit_author as fallback)
try:
    out("commit_data.with_commit_author", rows(f"""
        WITH project_mapping AS (
          SELECT DISTINCT level_1, level_2, level_3, level_4,
                 exploded_project_url AS project_url
          FROM {FGVF} fg
          LATERAL VIEW explode_outer(fg.project_url) AS exploded_project_url
          WHERE kpi_uuids = '{UUID}'
            AND level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
            AND exploded_project_url IS NOT NULL
        )
        SELECT project_url, COUNT(DISTINCT commit_id) AS commits,
               COUNT(DISTINCT commit_author) AS authors
        FROM {COMMITS}
        WHERE commit_date BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
          AND project_url IN (SELECT project_url FROM project_mapping)
          AND SIZE(SPLIT(before_sha, ',')) = 1
        GROUP BY project_url
    """))
except Exception as e:
    out("commit_data.with_commit_author.error", str(e))
    # Try with cleansed_commit_author
    out("commit_data.with_cleansed_commit_author", rows(f"""
        WITH project_mapping AS (
          SELECT DISTINCT exploded_project_url AS project_url
          FROM {FGVF} fg
          LATERAL VIEW explode_outer(fg.project_url) AS exploded_project_url
          WHERE kpi_uuids = '{UUID}'
            AND level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
            AND exploded_project_url IS NOT NULL
        )
        SELECT project_url, COUNT(DISTINCT commit_id) AS commits,
               COUNT(DISTINCT cleansed_commit_author) AS authors
        FROM {COMMITS}
        WHERE commit_date BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
          AND project_url IN (SELECT project_url FROM project_mapping)
          AND SIZE(SPLIT(before_sha, ',')) = 1
        GROUP BY project_url
    """))

# 3. Simulate the full chain up to project_commit_velocity
try:
    out("commits_vs_space.final_rows", rows(f"""
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
          SELECT s.survey_id, exploded_level_value AS level_filter,
                 AVG(CASE WHEN TRY_CAST(s.answer AS INT) BETWEEN 1 AND 5
                          THEN (TRY_CAST(s.answer AS INT)-1)*25 ELSE 0 END) AS avg_score
          FROM {VIEW} s
          LATERAL VIEW explode(s.level_value) AS exploded_level_value
          WHERE level_name = 'level_3'
            AND arrays_overlap(level_value, array('demo-acme-corp'))
            AND survey_id LIKE 'demo-seed-space-%'
            AND DATE(last_submitted_timestamp) BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
          GROUP BY s.survey_id, exploded_level_value
        ),
        survey_project AS (
          SELECT DISTINCT ss.survey_id, ss.avg_score AS overall_space_score, pm.project_url
          FROM survey_scores ss
          JOIN project_mapping pm ON (ss.level_filter = pm.level_1
                                   OR ss.level_filter = pm.level_2
                                   OR ss.level_filter = pm.level_3
                                   OR ss.level_filter = pm.level_4)
        ),
        commit_metrics AS (
          SELECT project_url,
                 COUNT(DISTINCT commit_id) AS total_commits,
                 COUNT(DISTINCT commit_date) AS active_days
          FROM {COMMITS}
          WHERE commit_date BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
            AND project_url IN (SELECT project_url FROM project_mapping)
            AND SIZE(SPLIT(before_sha,',')) = 1
          GROUP BY project_url
        )
        SELECT sp.survey_id, ROUND(sp.overall_space_score,2) AS space_score,
               cm.project_url, cm.total_commits, cm.active_days
        FROM survey_project sp
        JOIN commit_metrics cm ON sp.project_url = cm.project_url
        WHERE cm.total_commits > 0
        ORDER BY sp.survey_id
    """, limit=20))
except Exception as e:
    out("commits_vs_space.final_rows.error", str(e))
