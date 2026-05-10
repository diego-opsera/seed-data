# Debug — walk junit_drill_down_table_data.sql stage by stage with the
# drill-down KPI UUID, to find where rows drop.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/debug_junit_pipeline.py").read())

import json
from datetime import date, timedelta

CATALOG = "playground_prod"

# The KPIs the FE actually sends:
JUNIT_KPIS = {
    "junit_insights_overview":              "cf4070d1-f6e9-4d9f-bcc9-cbf27d4aff40",
    "junit_insights_drilldown_table_data":  "31b68fd2-aaf6-4eac-9638-c14dc5c2ebf6",
    "junit_insights_tab_data_points":       "e4d0bff4-392a-4878-9b78-522f0557c31a",
    "junit_insights_alt":                   "ca246d18-2fb0-47bf-844b-321e221778bd",
}


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=50):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)[:300]}]


# ── Stage 0: confirm our junit rows exist + show their git_url
print("\n" + "=" * 70)
print("  Stage 0 — junit_test_suite_report rows we just seeded")
print("=" * 70)
out("our_junit_rows", rows(f"""
    SELECT git_url, COUNT(*) AS n_runs, MIN(created_at) AS earliest, MAX(created_at) AS latest
    FROM {CATALOG}.source_to_stage.junit_test_suite_report
    WHERE service_principal IN ('seed-data', 'seed-data-meridian')
    GROUP BY git_url
"""))


# ── Stage 1: filter_groups_1 CTE for each KPI UUID
print("\n" + "=" * 70)
print("  Stage 1 — filter_groups_1 CTE (project_url) for each JUnit KPI")
print("  If 0 rows for a UUID, the FE for that widget gets nothing")
print("=" * 70)
for label, uuid in JUNIT_KPIS.items():
    out(f"filter_groups_1.{label}", rows(f"""
        SELECT DISTINCT exploded_project_url AS project_url
        FROM (
          SELECT *, sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () AS off_cnt
          FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
          WHERE level_3 = 'demo-acme-corp'
            AND kpi_uuids = '{uuid}'
        ) v
        LATERAL VIEW explode(project_url) AS exploded_project_url
    """))


# ── Stage 2: source CTE — junit_test_suite_report ⋈ filter_groups_1
print("\n" + "=" * 70)
print("  Stage 2 — source CTE for drill-down KPI 31b68fd2 (Acme)")
print("=" * 70)
out("source.acme.drilldown", rows(f"""
    WITH filter_groups_1 AS (
      SELECT DISTINCT exploded_project_url AS project_url
      FROM (
        SELECT *
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        WHERE level_3 = 'demo-acme-corp'
          AND kpi_uuids = '31b68fd2-aaf6-4eac-9638-c14dc5c2ebf6'
      ) v
      LATERAL VIEW explode(project_url) AS exploded_project_url
    )
    SELECT rmda.git_url, ot.project_url, COUNT(*) AS n_rows
    FROM {CATALOG}.source_to_stage.junit_test_suite_report rmda
    JOIN filter_groups_1 ot ON rmda.git_url = ot.project_url
    WHERE rmda.git_url IS NOT NULL AND rmda.git_url <> '' AND ot.project_url IS NOT NULL
    GROUP BY rmda.git_url, ot.project_url
"""))


# ── Stage 3: full drill-down query result with realistic date range
print("\n" + "=" * 70)
print("  Stage 3 — full drill-down query (level_3='demo-acme-corp')")
print("=" * 70)
fd = (date.today() - timedelta(days=270)).isoformat()
td = date.today().isoformat()
out("drilldown.acme.full", rows(f"""
    WITH variables AS (SELECT TO_DATE('{fd}') AS start_date, TO_DATE('{td}') AS end_date),
    filter_groups_1 AS (
      SELECT DISTINCT exploded_project_url AS project_url
      FROM (
        SELECT *
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        WHERE level_3 = 'demo-acme-corp'
          AND kpi_uuids = '31b68fd2-aaf6-4eac-9638-c14dc5c2ebf6'
      ) v
      LATERAL VIEW explode(project_url) AS exploded_project_url
    ),
    filter_groups_2 AS (
      SELECT DISTINCT coalesce(exploded_pipeline_tags,'X') as pipeline_tags
      FROM (
        SELECT *
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        WHERE level_3 = 'demo-acme-corp'
          AND kpi_uuids = '31b68fd2-aaf6-4eac-9638-c14dc5c2ebf6'
      ) v
      LATERAL VIEW explode(pipeline_tags) AS exploded_pipeline_tags
    ),
    source AS (
      SELECT pipeline_id, step_id, run_count, test_name, passed_tests, errored_tests,
             skipped_tests, failed_tests, total_tests, created_at, summary,
             pipeline_name, step_name, rmda.pipeline_tags
      FROM {CATALOG}.source_to_stage.junit_test_suite_report rmda
      JOIN filter_groups_1 ot ON rmda.git_url = ot.project_url
      WHERE rmda.git_url IS NOT NULL AND rmda.git_url <> '' AND ot.project_url IS NOT NULL
      UNION
      SELECT pipeline_id, step_id, run_count, test_name, passed_tests, errored_tests,
             skipped_tests, failed_tests, total_tests, created_at, summary,
             pipeline_name, step_name, rmda.pipeline_tags
      FROM {CATALOG}.source_to_stage.junit_test_suite_report rmda
      JOIN filter_groups_2 ot ON (ot.pipeline_tags = 'X' or array_contains(rmda.pipeline_tags, ot.pipeline_tags))
    )
    SELECT pipeline_id, pipeline_name, step_id, step_name, run_count, COUNT(*) AS n
    FROM source
    WHERE created_at IS NOT NULL
      AND TO_DATE(created_at) BETWEEN (SELECT start_date FROM variables) AND (SELECT end_date FROM variables)
    GROUP BY pipeline_id, pipeline_name, step_id, step_name, run_count
    ORDER BY pipeline_name, run_count
"""))


# ── Stage 4: also test that filter_groups_2 (pipeline_tags) is empty,
#       since the SQL UNIONs both filter_groups
print("\n" + "=" * 70)
print("  Stage 4 — filter_groups_2 (pipeline_tags) for drill-down KPI")
print("  If non-empty rows have ot.pipeline_tags = 'X', the SECOND UNION half")
print("  joins ALL junit rows (including all 10k+ real rows)")
print("=" * 70)
out("filter_groups_2.acme.drilldown", rows(f"""
    SELECT DISTINCT coalesce(exploded_pipeline_tags,'X') AS pipeline_tags
    FROM (
      SELECT *
      FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
      WHERE level_3 = 'demo-acme-corp'
        AND kpi_uuids = '31b68fd2-aaf6-4eac-9638-c14dc5c2ebf6'
    ) v
    LATERAL VIEW explode(pipeline_tags) AS exploded_pipeline_tags
"""))


# ── Stage 5: verify our hardcoded drill-down UUID actually got wired
print("\n" + "=" * 70)
print("  Stage 5 — flattened view rows for drill-down KPI 31b68fd2 (any level)")
print("=" * 70)
out("flattened.drilldown_kpi_rows", rows(f"""
    SELECT level_1, level_3, kpi_uuids, project_url, project_name, pipeline_tags
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE kpi_uuids = '31b68fd2-aaf6-4eac-9638-c14dc5c2ebf6'
    LIMIT 10
"""))
