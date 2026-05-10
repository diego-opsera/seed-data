# Debug — comprehensive filter_groups + filter_values inspection.
#
# Why: the page's Project dropdown for Meridian shows 'demo-meridian'
# instead of the expected 'Meridian Analytics' (which is what our DORA
# meridian/insert.py wires as level_1). That means another row exists
# in filter_groups_unity with level_1='demo-meridian' that we don't own.
#
# This script dumps every filter_groups_unity / filter_values_unity row
# tied to the two demo orgs so we can see:
#   1. Are there duplicates from re-runs?
#   2. Are there pre-existing tenant rows we're not attaching to?
#   3. Which filter_group_id are our project_name values actually attached to?
#   4. Does the SQL the FE actually sends (whereClause = level_3 selection
#      without level_1) return rows from our wiring?
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/debug_filter_groups.py").read())

import json

CATALOG = "playground_prod"
FGU = f"{CATALOG}.master_data.filter_groups_unity"
FVU = f"{CATALOG}.master_data.filter_values_unity"
V   = f"{CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=200):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)}]


# ── 1. ALL filter_groups_unity rows where any level mentions our demo orgs ───
print("\n" + "=" * 70)
print("  1. filter_groups_unity rows touching our demo orgs (any level)")
print("=" * 70)

out("filter_groups_unity.touching_demo_orgs", rows(f"""
    SELECT id, filter_group_id,
           level_1, level_2, level_3, level_4, level_5,
           createdBy, createdAt, active, roles
    FROM {FGU}
    WHERE lower(coalesce(level_1, '')) RLIKE 'acme|meridian|demo'
       OR lower(coalesce(level_3, '')) RLIKE 'acme|meridian|demo'
       OR createdBy IN ('seed-data@demo.io', 'seed-data-meridian@demo.io')
    ORDER BY level_3, level_1, createdAt
"""))


# ── 2. Duplicate detection — same (level_1, level_3, createdBy) appearing > 1 ─
print("\n" + "=" * 70)
print("  2. Duplicate filter_groups (same level_3 + createdBy)")
print("=" * 70)

out("filter_groups_unity.duplicates", rows(f"""
    SELECT level_1, level_3, createdBy, COUNT(*) AS dup_count,
           collect_list(filter_group_id) AS filter_group_ids
    FROM {FGU}
    WHERE createdBy IN ('seed-data@demo.io', 'seed-data-meridian@demo.io')
    GROUP BY level_1, level_3, createdBy
    HAVING dup_count > 1
"""))


# ── 3. dropdown source — what fetch-levels.sql returns for Project (level_1) ──
# Simulates the FE's Project-dropdown query when Product=demo-meridian is set.
print("\n" + "=" * 70)
print("  3a. fetch-levels.sql simulation — Project dropdown when Product=demo-meridian")
print("=" * 70)
out("dropdown.project_for_meridian", rows(f"""
    SELECT DISTINCT level_1 FROM {FGU}
    WHERE level_3 = 'demo-meridian'
    ORDER BY level_1
"""))

print("\n" + "=" * 70)
print("  3b. fetch-levels.sql simulation — Project dropdown when Product=demo-acme-corp")
print("=" * 70)
out("dropdown.project_for_acme", rows(f"""
    SELECT DISTINCT level_1 FROM {FGU}
    WHERE level_3 = 'demo-acme-corp'
    ORDER BY level_1
"""))


# ── 4. ALL filter_values_unity rows for the filter_group_ids we found ────────
print("\n" + "=" * 70)
print("  4. filter_values_unity attached to demo-org filter_group_ids")
print("=" * 70)

out("filter_values_unity.for_demo_filter_groups", rows(f"""
    SELECT fv.filter_group_id, fv.tool_type, fv.filter_name,
           fv.filter_values, size(fv.kpi_uuids) AS n_kpis,
           fv.created_by, fv.sort_number
    FROM {FVU} fv
    WHERE fv.filter_group_id IN (
        SELECT filter_group_id FROM {FGU}
        WHERE lower(coalesce(level_1, '')) RLIKE 'acme|meridian|demo'
           OR lower(coalesce(level_3, '')) RLIKE 'acme|meridian|demo'
    )
    ORDER BY fv.filter_group_id, fv.sort_number
"""))


# ── 5. duplicate filter_values_unity from re-runs of code_reliability/insert ─
print("\n" + "=" * 70)
print("  5. Duplicate code-reliability filter_values rows")
print("=" * 70)

out("filter_values_unity.cr_duplicates", rows(f"""
    SELECT filter_group_id, tool_type, filter_name, COUNT(*) AS n
    FROM {FVU}
    WHERE created_by = 'seed-data-cr@demo.io'
    GROUP BY filter_group_id, tool_type, filter_name
    HAVING n > 1
"""))


# ── 6. What does the flattened view return for each scenario the FE sends? ──
print("\n" + "=" * 70)
print("  6a. Flattened view — Product=demo-acme-corp, Project=Acme Corp")
print("       (most-likely whereClause when both filters are set)")
print("=" * 70)
out("flattened.acme_both_filters_set", rows(f"""
    SELECT level_1, level_3, kpi_uuids, project_name, tool_type
    FROM {V}
    WHERE level_3 = 'demo-acme-corp' AND level_1 = 'Acme Corp'
      AND project_name IS NOT NULL AND size(project_name) > 0
    ORDER BY kpi_uuids
"""))

print("\n" + "=" * 70)
print("  6b. Flattened view — Product=demo-acme-corp ONLY (no Project)")
print("=" * 70)
out("flattened.acme_only_product_set", rows(f"""
    SELECT level_1, level_3, kpi_uuids, project_name, tool_type
    FROM {V}
    WHERE level_3 = 'demo-acme-corp'
      AND project_name IS NOT NULL AND size(project_name) > 0
    ORDER BY kpi_uuids
""", limit=50))

print("\n" + "=" * 70)
print("  6c. Flattened view — Product=demo-meridian ONLY")
print("=" * 70)
out("flattened.meridian_product_only", rows(f"""
    SELECT level_1, level_3, kpi_uuids, project_name, tool_type
    FROM {V}
    WHERE level_3 = 'demo-meridian'
      AND project_name IS NOT NULL AND size(project_name) > 0
    ORDER BY kpi_uuids
""", limit=50))


# ── 7. The full sonar_ratings_overview pipeline for the most-realistic
#       whereClause (level_3 only, no level_1). If empty, the dashboard's
#       Sonar Ratings widget will be empty for any user who picks Product=
#       demo-acme-corp without picking a Project.
print("\n" + "=" * 70)
print("  7. sonar_ratings_overview — measures⋈filter_groups for level_3 only")
print("=" * 70)
out("sonar_ratings.level_3_only.acme", rows(f"""
    WITH filter_groups AS (
        SELECT distinct nvl(project_name, 'x') project_name
        FROM (SELECT project_name AS project_names
              FROM (
                SELECT *, sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () AS off_cnt
                FROM {V}
                WHERE level_3 = 'demo-acme-corp'
                  AND kpi_uuids = '9a712182-3c09-44be-ab73-371ed2ef977a'
              ) v_filter_fg_offcnt
        ) sub
        LATERAL VIEW explode_outer(project_names) AS project_name
    )
    SELECT mt.org_name, mt.project_name, COUNT(*) AS n_measures_rows
    FROM {CATALOG}.base_datasets.asp_sonar_measures mt
    JOIN filter_groups ot ON lower(ot.project_name) = lower(mt.project_name)
    WHERE mt.org_name = 'demo-acme-direct'
    GROUP BY mt.org_name, mt.project_name
"""))

# ── 8. Just to be thorough — show the actual filter_values_unity row our
#       wiring created so we can confirm filter_group_id matches what the
#       FE's Acme dashboard would target.
print("\n" + "=" * 70)
print("  8. Our seed-data-cr@demo.io filter_values rows with their filter_group's hierarchy")
print("=" * 70)
out("our_filter_values_with_hierarchy", rows(f"""
    SELECT fg.level_1, fg.level_3, fg.createdBy AS fg_createdBy,
           fv.tool_type, fv.filter_name, fv.filter_values, size(fv.kpi_uuids) AS n_kpis
    FROM {FVU} fv
    JOIN {FGU} fg ON fg.filter_group_id = fv.filter_group_id
    WHERE fv.created_by = 'seed-data-cr@demo.io'
    ORDER BY fg.level_1, fv.tool_type
"""))
