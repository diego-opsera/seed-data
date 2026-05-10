# Debug — walk the was_overview.sql pipeline against our seed data, stage
# by stage, to see which CTE drops rows. Especially probes the
# to_timestamp(... , 'dd/MM/yyyy hh:mm a') comparison that I had wrong
# (was emitting MM/dd format with lowercase am/pm).
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/debug_was_pipeline.py").read())

import json
from datetime import date, timedelta

CATALOG = "playground_prod"
WAS_KPI = "6fb402d7-bc63-4943-bd46-c72992e339da"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=50):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)[:300]}]


# Use rolling-90d so any reasonable dashboard date filter would include our scans
today = date.today()
fd = (today - timedelta(days=90)).isoformat()
td = today.isoformat()

# ── Stage 1: filter_groups CTE
print("\n" + "=" * 70)
print("  Stage 1 — filter_groups CTE for level_3='demo-acme-corp', WAS KPI")
print("=" * 70)
out("filter_groups.acme", rows(f"""
    SELECT project_name FROM (
      SELECT *, sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () AS off_cnt
      FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
      WHERE level_3 = 'demo-acme-corp'
        AND kpi_uuids = '{WAS_KPI}'
    ) v_filter_fg_offcnt
"""))


# ── Stage 2: source CTE — raw_invicti_data JOINed
print("\n" + "=" * 70)
print("  Stage 2 — source CTE (raw_invicti_data ⋈ filter_groups, State='Complete')")
print("=" * 70)
out("source.acme", rows(f"""
    WITH filter_groups AS (
        SELECT project_name FROM (
          SELECT *, sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () AS off_cnt
          FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
          WHERE level_3 = 'demo-acme-corp'
            AND kpi_uuids = '{WAS_KPI}'
        ) v
    )
    SELECT WebsiteId, WebsiteName, State, InitiatedAt, InitiatedTime, ThreatLevel
    FROM {CATALOG}.source_to_stage.raw_invicti_data s
    JOIN filter_groups f ON array_contains(f.project_name, WebsiteName) IS TRUE
    WHERE State = 'Complete'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY WebsiteId ORDER BY InitiatedAt DESC) = 1
"""))


# ── Stage 3: final CTE filter — checks the to_timestamp comparison
print("\n" + "=" * 70)
print("  Stage 3a — to_timestamp parse check on InitiatedTime + LastSeenDate")
print("  (must NOT be NULL or the LastSeenDate >= InitiatedTime filter drops rows)")
print("=" * 70)
out("ts_parse.invicti_data", rows(f"""
    SELECT WebsiteName, InitiatedTime,
           to_timestamp(InitiatedTime, 'dd/MM/yyyy hh:mm a') AS parsed_initiated_time
    FROM {CATALOG}.source_to_stage.raw_invicti_data
    WHERE record_inserted_by IN ('seed-data', 'seed-data-meridian')
"""))

out("ts_parse.invicti_issues", rows(f"""
    SELECT WebsiteName, Severity, IsPresent, LastSeenDate,
           to_timestamp(LastSeenDate, 'dd/MM/yyyy hh:mm a') AS parsed_last_seen
    FROM {CATALOG}.source_to_stage.raw_invicti_all_issues
    WHERE record_inserted_by IN ('seed-data', 'seed-data-meridian')
""", limit=20))


# ── Stage 4: full final CTE
print("\n" + "=" * 70)
print(f"  Stage 4 — final CTE for fromDate='{fd}' toDate='{td}'")
print("=" * 70)
out("final.acme", rows(f"""
    WITH input_params AS (SELECT TO_DATE('{fd}') AS start_date, TO_DATE('{td}') AS end_date),
    filter_groups AS (
        SELECT project_name FROM (
          SELECT *, sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () AS off_cnt
          FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
          WHERE level_3 = 'demo-acme-corp' AND kpi_uuids = '{WAS_KPI}'
        ) v
    ),
    source AS (
        SELECT DISTINCT WebsiteId, WebsiteName, WebsiteUrl, TargetUrl, TargetPath,
                        CONCAT(TargetUrl, TargetPath) AS path_url,
                        State, InitiatedTime, InitiatedAt, ThreatLevel
        FROM {CATALOG}.source_to_stage.raw_invicti_data s
        JOIN filter_groups f ON array_contains(f.project_name, WebsiteName) IS TRUE
        WHERE State = 'Complete'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY WebsiteId ORDER BY InitiatedAt DESC) = 1
    )
    SELECT DISTINCT f.WebsiteName, f.path_url, IsPresent, Severity, Title, LastSeenDate,
           f.InitiatedTime,
           TO_DATE(f.InitiatedAt) AS initiated_date,
           to_timestamp(LastSeenDate, 'dd/MM/yyyy hh:mm a') >=
             to_timestamp(f.InitiatedTime, 'dd/MM/yyyy hh:mm a') AS ts_compare_passes
    FROM source f
    LEFT JOIN {CATALOG}.source_to_stage.raw_invicti_all_issues a ON f.WebsiteId = a.WebsiteId
    WHERE a.Severity NOT IN ('Information', 'BestPractice') AND IsPresent = true
      AND f.InitiatedAt IS NOT NULL
      AND TO_DATE(f.InitiatedAt) BETWEEN (SELECT start_date FROM input_params) AND (SELECT end_date FROM input_params)
      AND to_timestamp(LastSeenDate, 'dd/MM/yyyy hh:mm a') >= to_timestamp(f.InitiatedTime, 'dd/MM/yyyy hh:mm a')
""", limit=30))


print("\n" + "=" * 70)
print("  RESULTS NOTES")
print("=" * 70)
print("""
- Stage 1 should return our project_name array.
- Stage 2 should return one Complete scan per project.
- Stage 3a should show parsed_initiated_time / parsed_last_seen as
  non-NULL timestamps. If NULL, the format mismatch is still a problem.
- Stage 4 should return rows. Each `ts_compare_passes` should be TRUE.

If Stage 4 returns rows here but the dashboard widget is still empty,
the issue is the dashboard's date range filter — try widening it.
""")
