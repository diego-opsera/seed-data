# Debug — run an approximation of sonar_ratings_overview.sql against our
# seed data, with a fixed whereClause + KPI UUID, to see if the data
# actually flows through to a result. If this returns rows but the dashboard
# is empty, the problem is on the FE side (KPI UUID mismatch, hierarchy
# toggle, or hierarchy selection). If it returns 0 rows, the seed data
# itself is missing something the SQL needs.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/debug_sonar_widget.py").read())

import json

CATALOG = "playground_prod"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=20):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)}]


# ── 1. Walk the SQL stages bottom-up. Each stage exposes one JOIN. If any
#    returns 0 rows, that's where the dashboard breaks.

print("\n" + "=" * 70)
print("  STAGE 1 — does our filter_groups CTE return rows for level_1='Acme Corp'?")
print("  (this is what the dashboard sends when user selects 'Acme Corp')")
print("=" * 70)

fg_acme = rows(f"""
    SELECT distinct nvl(project_name, 'x') project_name,
                    nvl(pipeline_name, 'x') pipeline_name,
                    nvl(pipeline_tag, 'x') pipeline_tag,
                    project_team_name
    FROM (SELECT project_name as project_names,
                 pipeline_name as pipeline_names,
                 pipeline_tags,
                 level_1 as project_team_name
          FROM (
            SELECT *, sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () AS off_cnt
            FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
            WHERE level_1 = 'Acme Corp'
              AND kpi_uuids = '9a712182-3c09-44be-ab73-371ed2ef977a'
          ) v_filter_fg_offcnt
    ) sonar_kpi_subq
    LATERAL VIEW explode_outer(project_names) AS project_name
    LATERAL VIEW explode_outer(pipeline_names) AS pipeline_name
    LATERAL VIEW explode_outer(pipeline_tags) AS pipeline_tag
""")
out("filter_groups.acme_for_sonar_ratings_overview_kpi", fg_acme)

# Repeat for Meridian
print("\n" + "=" * 70)
print("  STAGE 1b — same query but level_1='Meridian Analytics'")
print("=" * 70)

fg_mer = rows(f"""
    SELECT distinct nvl(project_name, 'x') project_name
    FROM (SELECT project_name as project_names
          FROM (
            SELECT *, sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () AS off_cnt
            FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
            WHERE level_1 = 'Meridian Analytics'
              AND kpi_uuids = '9a712182-3c09-44be-ab73-371ed2ef977a'
          ) v
    ) sub
    LATERAL VIEW explode_outer(project_names) AS project_name
""")
out("filter_groups.meridian_for_sonar_ratings_overview_kpi", fg_mer)

# ── 2. Confirm the seed measures rows JOIN to the filter_groups
print("\n" + "=" * 70)
print("  STAGE 2 — measures JOIN filter_groups (Acme)")
print("=" * 70)

stage2 = rows(f"""
    WITH variables AS (
        SELECT TO_DATE('2025-01-01') AS start_date, TO_DATE('2026-12-31') AS end_date
    ),
    filter_groups AS (
        SELECT distinct nvl(project_name, 'x') project_name
        FROM (SELECT project_name AS project_names
              FROM (
                SELECT *, sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () AS off_cnt
                FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
                WHERE level_1 = 'Acme Corp'
                  AND kpi_uuids = '9a712182-3c09-44be-ab73-371ed2ef977a'
              ) v_filter_fg_offcnt
        ) sonar_kpi_subq
        LATERAL VIEW explode_outer(project_names) AS project_name
    )
    SELECT mt.org_name, mt.project_name, mt.branch, COUNT(*) AS n
    FROM {CATALOG}.base_datasets.asp_sonar_measures mt
    JOIN filter_groups ot ON lower(ot.project_name) = lower(mt.project_name)
    WHERE mt.last_analysis_date BETWEEN (SELECT start_date FROM variables) AND (SELECT end_date FROM variables)
      AND mt.org_name = 'demo-acme-direct'
    GROUP BY mt.org_name, mt.project_name, mt.branch
""")
out("stage2.measures_join_filter_groups.acme", stage2)

# ── 3. Stage 3: Sonar Ratings final aggregation (acme)
print("\n" + "=" * 70)
print("  STAGE 3 — full sonar_ratings_overview-shape result for Acme")
print("=" * 70)

stage3 = rows(f"""
    WITH variables AS (
        SELECT TO_DATE('2025-01-01') AS start_date, TO_DATE('2026-12-31') AS end_date
    ),
    filter_groups AS (
        SELECT distinct nvl(project_name, 'x') project_name
        FROM (SELECT project_name AS project_names
              FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
              WHERE level_1 = 'Acme Corp'
                AND kpi_uuids = '9a712182-3c09-44be-ab73-371ed2ef977a'
        ) sub
        LATERAL VIEW explode_outer(project_names) AS project_name
    ),
    primary_source AS (
        SELECT distinct mt.org_name, mt.project_name, mt.branch
        FROM {CATALOG}.base_datasets.asp_sonar_measures mt
        JOIN filter_groups ot ON lower(ot.project_name) = lower(mt.project_name)
        WHERE mt.last_analysis_date BETWEEN (SELECT start_date FROM variables) AND (SELECT end_date FROM variables)
    ),
    sonar_measure_source AS (
        SELECT asp.project_bugs_value, asp.project_reliability_rating_value,
               asp.project_sqale_rating_value, asp.project_name, asp.branch,
               asp.org_name, asp.source_record_insert_datetime,
               row_number() OVER (PARTITION BY asp.org_name, asp.project_name, asp.branch
                                  ORDER BY asp.last_analysis_date DESC) AS rank
        FROM {CATALOG}.base_datasets.asp_sonar_measures asp
        JOIN primary_source ps ON asp.org_name = ps.org_name
                              AND asp.project_name = ps.project_name
                              AND asp.branch = ps.branch
        WHERE asp.last_analysis_date <= (SELECT end_date FROM variables)
        QUALIFY row_number() OVER (PARTITION BY asp.org_name, asp.project_name, asp.branch
                                   ORDER BY asp.last_analysis_date DESC) <= 2
    ),
    issues_x_measures AS (
        SELECT std.type, std.severity, smsd.project_bugs_value bugs,
               smsd.project_reliability_rating_value AS rel_rating,
               smsd.project_sqale_rating_value AS sqale_rating,
               smsd.project_name, smsd.branch AS branch_name, smsd.rank,
               CASE WHEN smsd.rank = 1 THEN 'current' ELSE 'previous' END AS time_period
        FROM {CATALOG}.base_datasets.asp_sonar_issues std
        JOIN sonar_measure_source smsd
          ON std.org_name = smsd.org_name
         AND std.project = smsd.project_name
         AND std.branch = smsd.branch
         AND std.source_record_insert_datetime = smsd.source_record_insert_datetime
        WHERE smsd.rank <= 2 AND std.status IN ('OPEN','REOPENED','CONFIRMED')
    )
    SELECT project_name, branch_name, time_period, type,
           COUNT(*) AS issue_count,
           MAX(rel_rating) AS reliability_rating,
           MAX(sqale_rating) AS maintainability_rating
    FROM issues_x_measures
    GROUP BY project_name, branch_name, time_period, type
    ORDER BY project_name, time_period, type
""")
out("stage3.sonar_ratings_full_acme", stage3)

# ── 4. Diagnostic — what's the raw view say for our orgs at all KPI UUIDs?
print("\n" + "=" * 70)
print("  STAGE 0 — what KPI UUIDs are wired to OUR seeded project_name values?")
print("=" * 70)

stage0 = rows(f"""
    SELECT level_1, kpi_uuids, project_name
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_1 IN ('Acme Corp', 'Meridian Analytics')
      AND project_name IS NOT NULL
      AND size(project_name) > 0
      AND array_contains(project_name, 'backend')
       OR array_contains(project_name, 'data-platform')
    ORDER BY level_1, kpi_uuids
""", limit=50)
out("stage0.demo_org_filter_rows_for_our_projects", stage0)
