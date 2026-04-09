# Diagnostic: test a single space survey INSERT row, then verify view picks it up
# Run: exec(open("/tmp/seed-data/notebooks/devex/diag_space_3.py").read())

import json

CATALOG = "playground_prod"
BASE    = f"{CATALOG}.source_to_stage.survey_details_with_responses"
VIEW    = f"{CATALOG}.source_to_stage.v_survey_details_with_responses"

def rows(q, limit=10):
    return [r.asDict() for r in spark.sql(q).limit(limit).collect()]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

# Step 1: insert a single test row
spark.sql(f"""
    INSERT INTO {BASE}
      (survey_id, survey_name, description, filters,
       form_id, question_id, question,
       answer_value, responseId, lastSubmittedTime)
    VALUES (
      'demo-seed-space-test',
      'SPACE Test Survey',
      'test row',
      NAMED_STRUCT(
        'level_1', ARRAY('Acme Corp'),
        'level_2', NULL,
        'level_3', ARRAY('demo-acme-corp'),
        'level_4', NULL,
        'level_5', NULL,
        'svp', NULL,
        'vp', NULL,
        'director', NULL,
        'supervisor', NULL
      ),
      'form-test-001',
      '257bb6de',
      'Test question',
      '4',
      'resp-test-user1',
      TIMESTAMP '2025-06-30 12:00:00'
    )
""")
print("INSERT done")

# Step 2: verify in base table
out("base.test_row", rows(f"""
    SELECT survey_id, responseId, answer_value, lastSubmittedTime
    FROM {BASE} WHERE survey_id = 'demo-seed-space-test'
"""))

# Step 3: verify view shows the row with correct level_name/level_value
out("view.test_row", rows(f"""
    SELECT survey_id, level_name, level_value, answer, last_submitted_timestamp
    FROM {VIEW} WHERE survey_id = 'demo-seed-space-test'
"""))

# Step 4: verify spaceDashboardFilterClause would match
out("view.space_filter_test", rows(f"""
    SELECT COUNT(*) AS n FROM {VIEW}
    WHERE survey_id = 'demo-seed-space-test'
      AND level_name = 'level_3'
      AND arrays_overlap(level_value, array('demo-acme-corp'))
"""))

# Cleanup the test row
spark.sql(f"DELETE FROM {BASE} WHERE survey_id = 'demo-seed-space-test'")
print("\nTest row cleaned up.")
