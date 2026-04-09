# Diagnostic: debug "SPACE Score vs Commit Velocity" empty chart
# Run: exec(open("/tmp/seed-data/notebooks/devex/diag_space_4.py").read())

import json

CATALOG  = "playground_prod"
FGVF     = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"
VIEW     = f"{CATALOG}.source_to_stage.v_survey_details_with_responses"
COMMITS  = f"{CATALOG}.base_datasets.commits_rest_api"
UUID     = "space_d4e5f6g7-h8i9-0123-defg-456789012345"  # commits_vs_space

def rows(q, limit=10):
    return [r.asDict() for r in spark.sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

# 1. Does project_mapping return rows for commits_vs_space UUID?
out("project_mapping.rows", rows(f"""
    SELECT DISTINCT level_1, level_2, level_3, level_4,
           exploded_project_url AS project_url
    FROM {FGVF} fg
    LATERAL VIEW explode_outer(fg.project_url) AS exploded_project_url
    WHERE kpi_uuids = '{UUID}'
      AND level_1 = 'Acme Corp' AND level_3 = 'demo-acme-corp'
      AND exploded_project_url IS NOT NULL
"""))

# 2. Survey space scores — does our data appear via spaceDashboardFilterClause?
out("survey_space_scores.count", rows(f"""
    SELECT COUNT(*) AS n, COUNT(DISTINCT response_id) AS unique_resp
    FROM {VIEW}
    WHERE level_name = 'level_3'
      AND arrays_overlap(level_value, array('demo-acme-corp'))
      AND survey_id LIKE 'demo-seed-space-%'
"""))

# 3. After LATERAL VIEW explode(level_value): distinct level_filter values
out("survey.level_filter_values", rows(f"""
    SELECT DISTINCT exploded_level_value AS level_filter
    FROM {VIEW}
    LATERAL VIEW explode(level_value) AS exploded_level_value
    WHERE level_name = 'level_3'
      AND arrays_overlap(level_value, array('demo-acme-corp'))
      AND survey_id LIKE 'demo-seed-space-%'
"""))

# 4. Does 'demo-acme-corp' appear as any level column in project_mapping?
out("project_mapping.level_values", rows(f"""
    SELECT DISTINCT level_1, level_2, level_3, level_4
    FROM {FGVF}
    WHERE kpi_uuids = '{UUID}'
      AND level_1 = 'Acme Corp'
    LIMIT 5
"""))

# 5. Commit project_urls — do they match what's in filter_values_unity?
out("commits.project_urls", rows(f"""
    SELECT DISTINCT project_url, COUNT(*) AS n
    FROM {COMMITS}
    WHERE org_name = 'demo-acme-direct'
    GROUP BY project_url
"""))
