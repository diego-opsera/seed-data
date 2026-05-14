# Developer Language And Editor Usage dashboard — diagnose 0% rows
#
# The screenshot shows model names like
# 'copilot-prod-finetune-centralus.opsera-xr-...' with 0% across the board.
# Those aren't our seeded models. Two hypotheses:
#   (A) the dashboard's filter context is showing Opsera production data,
#       not demo-acme-direct — our seed data is fine but invisible to this view.
#   (B) our seed model names don't match what the dashboard expects, so our
#       rows aren't aggregating into the dropdown / chart at all.
#
# This script tells us which.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/core/debug_dev_lang_editor.py").read())

CATALOG = "playground_prod"

print()
print("=" * 78)
print("1. enterprise_user_language_model_level_copilot_metrics (driving table)")
print("=" * 78)

print("\n-- Our seeded rows (enterprise_id = 999999) --")
spark.sql(f"""
  SELECT enterprise, COUNT(*) AS rows,
         COUNT(DISTINCT model) AS n_models,
         MIN(usage_date) AS min_date,
         MAX(usage_date) AS max_date
  FROM {CATALOG}.base_datasets.enterprise_user_language_model_level_copilot_metrics
  WHERE enterprise_id = 999999
  GROUP BY enterprise
""").show(truncate=False)

print("\n-- DISTINCT model values for our enterprise --")
spark.sql(f"""
  SELECT DISTINCT model
  FROM {CATALOG}.base_datasets.enterprise_user_language_model_level_copilot_metrics
  WHERE enterprise_id = 999999
  ORDER BY model
""").show(truncate=False)

print("\n-- All enterprises that have data in the dashboard's date range --")
spark.sql(f"""
  SELECT enterprise_id, enterprise, COUNT(*) AS rows,
         COUNT(DISTINCT model) AS n_models
  FROM {CATALOG}.base_datasets.enterprise_user_language_model_level_copilot_metrics
  WHERE usage_date BETWEEN DATE '2025-08-16' AND DATE '2026-05-12'
  GROUP BY enterprise_id, enterprise
  ORDER BY rows DESC
  LIMIT 20
""").show(truncate=False)

print("\n-- Top 15 distinct models in the table (any enterprise) --")
spark.sql(f"""
  SELECT model, COUNT(*) AS rows, COUNT(DISTINCT enterprise_id) AS n_orgs
  FROM {CATALOG}.base_datasets.enterprise_user_language_model_level_copilot_metrics
  WHERE usage_date BETWEEN DATE '2025-08-16' AND DATE '2026-05-12'
  GROUP BY model
  ORDER BY rows DESC
  LIMIT 15
""").show(truncate=False)

print()
print("=" * 78)
print("2. github_copilot_metrics_ide_org_level (drives the model-name DROPDOWN)")
print("=" * 78)

print("\n-- Our seeded rows --")
spark.sql(f"""
  SELECT org_name, COUNT(*) AS rows,
         COUNT(DISTINCT ide_code_completion_model_name) AS n_completion_models,
         COUNT(DISTINCT ide_chat_model_name) AS n_chat_models
  FROM {CATALOG}.base_datasets.github_copilot_metrics_ide_org_level
  WHERE org_name = 'demo-acme-direct'
  GROUP BY org_name
""").show(truncate=False)

print("\n-- DISTINCT ide_code_completion_model_name values for our org --")
spark.sql(f"""
  SELECT DISTINCT ide_code_completion_model_name
  FROM {CATALOG}.base_datasets.github_copilot_metrics_ide_org_level
  WHERE org_name = 'demo-acme-direct'
  ORDER BY 1
""").show(truncate=False)

print("\n-- Top 15 distinct ide_code_completion_model_name across all orgs --")
spark.sql(f"""
  SELECT ide_code_completion_model_name, COUNT(*) AS rows,
         COUNT(DISTINCT org_name) AS n_orgs
  FROM {CATALOG}.base_datasets.github_copilot_metrics_ide_org_level
  WHERE copilot_usage_date BETWEEN DATE '2025-08-16' AND DATE '2026-05-12'
  GROUP BY ide_code_completion_model_name
  ORDER BY rows DESC
  LIMIT 15
""").show(truncate=False)

print()
print("=" * 78)
print("3. Verdict")
print("=" * 78)
print("""
Read sections 1 and 2 together:

  - Section 1 'our rows' = 0  → our language_model_level data isn't seeded
                                (run core/insert.py).
  - Section 1 distinct models for us = gpt-4o, claude-3.7-sonnet, etc.  → seed is fine;
                                the dashboard is filtering us out OR using a different field.
  - Section 1 'all enterprises' shows OPSERA / HW / OTHER tenants with the
    'copilot-prod-finetune-centralus.opsera-*' models  → confirms the dashboard
    is showing OTHER orgs' data; the fix is in the dashboard's filter context, not the seed.
  - Section 2 distinct ide_code_completion_model_name for us = 'default'  → our seed
    uses 'default' for this column, which doesn't match the dashboard's
    expected display values. Changing to 'GitHub Copilot Open AI' would
    surface our org if the dashboard's filter context does include us.
""")
