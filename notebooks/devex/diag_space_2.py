# Diagnostic: debug SPACE metrics "No data available"
# Run: exec(open("/tmp/seed-data/notebooks/devex/diag_space_2.py").read())

import json

CATALOG = "playground_prod"
VIEW    = f"{CATALOG}.source_to_stage.v_survey_details_with_responses"
BASE    = f"{CATALOG}.source_to_stage.survey_details_with_responses"

def rows(q, limit=10):
    return [r.asDict() for r in spark.sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

# 1. Row count in base table
out("base.row_count", rows(f"""
    SELECT COUNT(*) AS n FROM {BASE} WHERE survey_id LIKE 'demo-seed-space-%'
"""))

# 2. Schema of base table (shows actual column names and types)
out("base.schema", {r["col_name"]: r["data_type"]
    for r in spark.sql(f"DESCRIBE {BASE}").collect()
    if r["col_name"] and not r["col_name"].startswith("#")})

# 3. Schema of the view
out("view.schema", {r["col_name"]: r["data_type"]
    for r in spark.sql(f"DESCRIBE {VIEW}").collect()
    if r["col_name"] and not r["col_name"].startswith("#")})

# 4. Sample rows from the view (no filter) — check columns exposed
out("view.sample_raw", rows(f"SELECT * FROM {VIEW} LIMIT 3"))

# 5. Distinct level_name values present in view for our data
try:
    out("view.distinct_level_names", rows(f"""
        SELECT DISTINCT level_name, level_value
        FROM {VIEW}
        WHERE survey_id LIKE 'demo-seed-space-%'
    """, limit=20))
except Exception as e:
    out("view.distinct_level_names.error", str(e))

# 6. Try the spaceDashboardFilterClause directly
try:
    out("view.space_filter_match", rows(f"""
        SELECT COUNT(*) AS n
        FROM {VIEW}
        WHERE level_name = 'level_3'
          AND arrays_overlap(level_value, array('demo-acme-corp'))
          AND survey_id LIKE 'demo-seed-space-%'
    """))
except Exception as e:
    out("view.space_filter_match.error", str(e))

# 7. Check lastSubmittedTime / last_submitted_timestamp stored value
out("base.sample_timestamps", rows(f"""
    SELECT survey_id, responseId, question_id, answer_value, lastSubmittedTime
    FROM {BASE}
    WHERE survey_id LIKE 'demo-seed-space-%'
    LIMIT 3
"""))

# 8. View definition
try:
    out("view.definition", rows(f"SHOW CREATE TABLE {VIEW}"))
except Exception as e:
    out("view.definition.error", str(e))
