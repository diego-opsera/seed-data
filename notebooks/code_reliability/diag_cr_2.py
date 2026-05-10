# Diagnostic round 2 — filter_groups_unity wiring for Code Reliability
#
# The Sonar / Twistlock / WAS SQL queries all join the source row's project_name
# to the EXPLODED project_name array of master_data.filter_groups_unity. For our
# demo orgs to surface in the dashboard we need the project_name values our
# generators will emit to also exist in the demo-* filter_groups_unity rows.
#
# This script:
#   1. Dumps the filter_groups_unity rows for our two demo orgs
#   2. Lists their distinct project_name array values
#   3. Lists distinct project_name values already present in DORA + value_stream
#      tables (those are our "real" project_name values today)
#   4. Notes any overlap / gap
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_2.py").read())

import json

CATALOG = "playground_prod"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=50):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)}]


# ── 1. filter_groups_unity for demo orgs ────────────────────────────────────

FGU = f"{CATALOG}.master_data.filter_groups_unity"

out("fgu.demo_rows", rows(f"""
    SELECT id, level_1, level_2, level_3, level_4, level_5, createdBy, filter_group_id
    FROM {FGU}
    WHERE createdBy IN ('seed-data@demo.io', 'seed-data-meridian@demo.io')
       OR lower(coalesce(level_1, '')) RLIKE 'acme|meridian'
       OR lower(coalesce(level_3, '')) RLIKE 'demo'
    ORDER BY createdBy, level_1
"""))

# ── 2. filter_values_unity for those filter groups ──────────────────────────
FVU = f"{CATALOG}.master_data.filter_values_unity"

out("fvu.schema", {
    r["col_name"]: r["data_type"]
    for r in spark.sql(f"DESCRIBE TABLE {FVU}").collect()
    if r["col_name"] and not r["col_name"].startswith("#")
})

out("fvu.demo_rows.by_field", rows(f"""
    SELECT field_name, COUNT(*) AS n
    FROM {FVU}
    WHERE created_by IN ('seed-data@demo.io', 'seed-data-meridian@demo.io')
    GROUP BY field_name
    ORDER BY n DESC
"""))

out("fvu.demo_rows.project_name", rows(f"""
    SELECT field_name, field_values, kpi_id, source, created_by
    FROM {FVU}
    WHERE created_by IN ('seed-data@demo.io', 'seed-data-meridian@demo.io')
      AND field_name IN ('project_name', 'project_url', 'project_team_name')
    ORDER BY created_by, field_name
""", limit=200))

# ── 3. project_name values already present in seeded data ──────────────────
# These are the names our existing generators emit. Sonar/Twistlock/WAS will
# have to use values from this set (or we add new ones to filter_groups_unity).

print("\n" + "=" * 70)
print("  Existing seeded project_name values (sources for future Sonar joins)")
print("=" * 70)

# DORA pipeline_activities
out("pipeline_activities.project_url.demo", rows(f"""
    SELECT DISTINCT project_url
    FROM {CATALOG}.base_datasets.pipeline_activities
    WHERE record_inserted_by IN ('seed-data', 'seed-data-meridian', 'seed-data-value-stream')
    LIMIT 50
"""))

# value_stream offerings_jira_pipeline_details
out("offerings_jira_pipeline_details.project_name.demo", rows(f"""
    SELECT DISTINCT jira_project, sbg, gbe, offering, org_name
    FROM {CATALOG}.user_working.offerings_jira_pipeline_details
    WHERE org_name IN ('demo-acme-direct', 'demo-meridian')
"""))

# itsm issues project_name
out("mt_itsm_issues_current.project_name.demo", rows(f"""
    SELECT DISTINCT project_name, customer_id
    FROM {CATALOG}.transform_stage.mt_itsm_issues_current
    WHERE customer_id IN ('demo-acme-direct', 'demo-meridian')
    LIMIT 30
"""))

# ── 4. flattened view (used by SQL queries) — verify our orgs surface ──────
FGV = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"

out("v_fgv_kpi_flattened.schema_top", {
    r["col_name"]: r["data_type"]
    for r in spark.sql(f"DESCRIBE TABLE {FGV}").collect()[:30]
    if r["col_name"] and not r["col_name"].startswith("#")
})

out("v_fgv_kpi_flattened.demo_sample", rows(f"""
    SELECT level_1, level_2, level_3, project_name, pipeline_name, pipeline_tag
    FROM {FGV}
    WHERE lower(coalesce(level_1, '')) RLIKE 'acme|meridian'
       OR lower(coalesce(level_3, '')) RLIKE 'demo'
    LIMIT 40
"""))

print("\n" + "=" * 70)
print("  Done. Next step: read these results back and decide:")
print("    - Which project_name strings the new Sonar/Twistlock/WAS generators emit")
print("    - Whether to add new filter_values_unity rows for them, or reuse existing")
print("=" * 70)
