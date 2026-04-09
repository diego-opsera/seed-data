# Diagnostic: discover v_survey_details_with_responses view structure
# Run: exec(open("/tmp/seed-data/notebooks/devex/diag_space_1.py").read())

import json

CATALOG = "playground_prod"

def rows(q, limit=20):
    return [r.asDict() for r in spark.sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

VIEW = f"{CATALOG}.source_to_stage.v_survey_details_with_responses"

# 1. Schema of the view
out("view.schema", {r["col_name"]: r["data_type"]
    for r in spark.sql(f"DESCRIBE {VIEW}").collect()
    if r["col_name"] and not r["col_name"].startswith("#")})

# 2. View definition (shows the underlying SQL)
out("view.definition", rows(f"SHOW CREATE TABLE {VIEW}"))

# 3. Sample rows (if any data exists)
out("view.sample_rows", rows(f"SELECT * FROM {VIEW} LIMIT 5"))

# 4. What level_names exist?
try:
    out("view.distinct_level_names", rows(f"""
        SELECT DISTINCT level_name, COUNT(*) AS n FROM {VIEW} GROUP BY level_name
    """))
except Exception as e:
    out("view.distinct_level_names.error", str(e))
