# Debug — run the FULL sonar_ratings_overview.sql pipeline against our seed
# data with different :fromDate, :toDate, and offering-toggle combos to pin
# down which combo returns rows. If any combo returns rows, we know the
# data side is fine and the dashboard's saved filter is the blocker.
#
# Run:
#   exec(open("/tmp/seed-data/notebooks/code_reliability/debug_sonar_full_pipeline.py").read())

import json
from datetime import date, timedelta

CATALOG = "playground_prod"


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def rows(q, limit=50):
    try:
        return [r.asDict(recursive=True) for r in spark.sql(q).limit(limit).collect()]
    except Exception as e:
        return [{"error": str(e)[:500]}]


SONAR_RATINGS_KPI = "9a712182-3c09-44be-ab73-371ed2ef977a"


def sonar_ratings(from_date: str, to_date: str,
                  where_clause: str,
                  is_offering_toggle: bool,
                  is_offering_enabled: bool):
    """Inline the sonar_ratings_overview.sql with literal values substituted
    in place of bind variables, so we can test combos that the FE might send."""
    toggle_lit  = "TRUE" if is_offering_toggle else "FALSE"
    enabled_lit = "TRUE" if is_offering_enabled else "FALSE"
    return rows(f"""
        WITH variables AS (
            SELECT TO_DATE('{from_date}') AS start_date,
                   TO_DATE('{to_date}')   AS end_date
        ),
        filter_groups AS (
            SELECT DISTINCT nvl(project_name, 'x') project_name
            FROM (SELECT project_name AS project_names
                  FROM (
                    SELECT *, sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () AS off_cnt
                    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
                    {where_clause}
                      AND kpi_uuids = '{SONAR_RATINGS_KPI}'
                    QUALIFY CASE WHEN NOT {toggle_lit} THEN TRUE
                                 ELSE CASE WHEN {enabled_lit}
                                            AND sum(CASE WHEN level_1 LIKE 'off:%' THEN 1 ELSE 0 END) OVER () > 0
                                           THEN level_1 LIKE 'off:%'
                                           ELSE level_1 NOT LIKE 'off:%' END END
                  ) v_filter_fg_offcnt
            ) sub
            LATERAL VIEW explode_outer(project_names) AS project_name
        ),
        primary_source AS (
            SELECT DISTINCT mt.org_name, mt.project_name, mt.branch
            FROM {CATALOG}.base_datasets.asp_sonar_measures mt
            JOIN filter_groups ot ON lower(ot.project_name) = lower(mt.project_name)
            WHERE mt.last_analysis_date BETWEEN (SELECT start_date FROM variables) AND (SELECT end_date FROM variables)
        ),
        sonar_measure_source AS (
            SELECT asp.project_name, asp.branch, asp.org_name,
                   asp.source_record_insert_datetime, asp.last_analysis_date,
                   row_number() OVER (PARTITION BY asp.org_name, asp.project_name, asp.branch
                                      ORDER BY asp.last_analysis_date DESC) AS rk
            FROM {CATALOG}.base_datasets.asp_sonar_measures asp
            JOIN primary_source ps ON asp.org_name = ps.org_name AND asp.project_name = ps.project_name AND asp.branch = ps.branch
            WHERE asp.last_analysis_date <= (SELECT end_date FROM variables)
        ),
        joined AS (
            SELECT std.type, smsd.project_name, smsd.last_analysis_date, smsd.rk
            FROM {CATALOG}.base_datasets.asp_sonar_issues std
            JOIN sonar_measure_source smsd
              ON std.org_name = smsd.org_name AND std.project = smsd.project_name
             AND std.branch = smsd.branch
             AND std.source_record_insert_datetime = smsd.source_record_insert_datetime
            WHERE smsd.rk <= 2 AND std.status IN ('OPEN','REOPENED','CONFIRMED')
        )
        SELECT project_name, type, COUNT(*) AS issue_count, MAX(last_analysis_date) AS latest_scan
        FROM joined
        GROUP BY project_name, type
        ORDER BY project_name, type
    """, limit=50)


# ── Test matrix: 4 date ranges × 4 toggle combos × 2 whereClauses ──────────
today = date.today()
ranges = [
    ("rolling_30d",  (today - timedelta(days=30)).isoformat(),  today.isoformat()),
    ("rolling_90d",  (today - timedelta(days=90)).isoformat(),  today.isoformat()),
    ("rolling_365d", (today - timedelta(days=365)).isoformat(), today.isoformat()),
    ("calendar_2024",                       "2024-01-01",      "2024-12-31"),
    ("calendar_2025",                       "2025-01-01",      "2025-12-31"),
]

where_clauses = [
    ("level_3_only",            "WHERE level_3 = 'demo-acme-corp'"),
    ("level_3_AND_level_1",     "WHERE level_3 = 'demo-acme-corp' AND level_1 = 'Acme Corp'"),
]

toggle_combos = [
    ("toggle_off",                   False, False),
    ("toggle_on_offering_off",       True,  False),
    ("toggle_on_offering_on",        True,  True),
]

print("\n" + "=" * 70)
print("  Sonar Ratings — combo matrix (returned issue_count summed)")
print("=" * 70)

results = {}
for date_label, fd, td in ranges:
    for wc_label, wc in where_clauses:
        for tg_label, tg, en in toggle_combos:
            r = sonar_ratings(fd, td, wc, tg, en)
            if r and "error" not in r[0]:
                total = sum(row.get("issue_count", 0) for row in r if "issue_count" in row)
            else:
                total = 0 if not r else f"ERROR"
            key = f"{date_label}__{wc_label}__{tg_label}"
            results[key] = total

out("combo_matrix.results", results)


# ── If any combo > 0, dump the rows for that one as a sanity check
print("\n" + "=" * 70)
print("  Best combo result detail (first non-zero)")
print("=" * 70)

for date_label, fd, td in ranges:
    for wc_label, wc in where_clauses:
        for tg_label, tg, en in toggle_combos:
            r = sonar_ratings(fd, td, wc, tg, en)
            if r and "error" not in r[0]:
                total = sum(row.get("issue_count", 0) for row in r if "issue_count" in row)
                if total > 0:
                    out(f"detail.{date_label}__{wc_label}__{tg_label}", r)
                    break
        else:
            continue
        break
    else:
        continue
    break


# ── Spot-check our latest scan dates per project (so we know if a 30-day
#       window includes them)
print("\n" + "=" * 70)
print("  Latest seeded scan date per project")
print("=" * 70)
out("latest_scan_per_project", rows(f"""
    SELECT org_name, project_name,
           MAX(last_analysis_date) AS latest_scan,
           MIN(last_analysis_date) AS earliest_scan,
           COUNT(*) AS n_scans
    FROM {CATALOG}.base_datasets.asp_sonar_measures
    WHERE record_inserted_by IN ('seed-data', 'seed-data-meridian')
    GROUP BY org_name, project_name
    ORDER BY org_name, project_name
"""))
