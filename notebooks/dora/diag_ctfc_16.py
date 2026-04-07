# CTFC round 16: simulate exact ctfc_chart.sql CTE chain with our data
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_16.py").read())

import json

CATALOG  = "playground_prod"
CTFC_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"
LEVEL_3  = "demo-acme-corp"

# Use a broad date range covering all our issue_updated dates
FROM_DATE = "2025-01-01"
TO_DATE   = "2026-04-07"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def count(q): return sql(q).collect()[0][0]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. Is hist populated? ─────────────────────────────────────────────────────
out("hist.seed_count", [{"n": count(f"""
    SELECT COUNT(*) FROM {CATALOG}.base_datasets.v_itsm_issues_hist
    WHERE record_inserted_by = 'seed-data'
""")}])

# ── 2. source CTE: issues from hist with itsm_source='jira' ──────────────────
out("source.count", [{"n": count(f"""
    SELECT COUNT(*) FROM {CATALOG}.base_datasets.v_itsm_issues_hist
    WHERE itsm_source = 'jira' AND record_inserted_by = 'seed-data'
""")}])

# ── 3. filtered_source: changelog_itemsfield='status' AND updated in range ───
out("filtered_source.count", [{"n": count(f"""
    SELECT COUNT(*) FROM {CATALOG}.base_datasets.v_itsm_issues_hist
    WHERE itsm_source = 'jira'
      AND record_inserted_by = 'seed-data'
      AND lower(issue_changelog_itemsfield) IN ('status', 'epic status')
      AND TO_DATE(issue_updated) BETWEEN DATE '{FROM_DATE}' AND DATE '{TO_DATE}'
""")}])

# ── 4. filter_groups CTE: view row for our org + KPI ─────────────────────────
out("filter_groups.rows", [r.asDict() for r in sql(f"""
    SELECT DISTINCT board_ids, issue_status, include_issue_types,
           explode_outer(project_name) AS project_name
    FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
    WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
""").collect()])

# ── 5. Full filtered_data join (core of the chart) ───────────────────────────
out("filtered_data.count", [{"n": count(f"""
    WITH source AS (
        SELECT issue_key, issue_status AS status, issue_status, issue_created,
               issue_updated, issue_updated AS resolved_date,
               issue_project, issue_type, issue_changelog_itemsfield,
               transform(board_info, x -> CAST(x.board_id AS STRING)) AS board_ids
        FROM {CATALOG}.base_datasets.v_itsm_issues_hist
        WHERE itsm_source = 'jira' AND record_inserted_by = 'seed-data'
    ),
    filtered_source AS (
        SELECT * FROM source
        WHERE lower(issue_changelog_itemsfield) IN ('status', 'epic status')
          AND TO_DATE(resolved_date) BETWEEN DATE '{FROM_DATE}' AND DATE '{TO_DATE}'
    ),
    filter_groups AS (
        SELECT DISTINCT board_ids, issue_status,
               nvl(include_issue_status, array()) AS include_issue_status,
               exclude_issue_status, include_issue_types,
               explode_outer(project_name) AS project_name
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
    )
    SELECT COUNT(*) FROM (
        SELECT s.*, f.issue_status AS filter_status, f.include_issue_status,
               f.exclude_issue_status, f.include_issue_types
        FROM filtered_source s
        JOIN filter_groups f
          ON (f.project_name = s.issue_project OR f.project_name IS NULL)
         AND (arrays_overlap(f.board_ids, s.board_ids) OR f.board_ids IS NULL)
         AND (f.board_ids IS NOT NULL OR f.project_name IS NOT NULL)
         AND f.issue_status IS NOT NULL
         AND ARRAY_CONTAINS(f.issue_status, s.status)
         AND IF(f.include_issue_types IS NOT NULL, ARRAY_CONTAINS(f.include_issue_types, s.issue_type), TRUE)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY s.issue_key ORDER BY s.issue_updated DESC) = 1
    )
""")}])

# ── 6. What issue_updated range does our hist data have? ─────────────────────
out("hist.date_range", [r.asDict() for r in sql(f"""
    SELECT MIN(issue_updated) AS min_updated, MAX(issue_updated) AS max_updated
    FROM {CATALOG}.base_datasets.v_itsm_issues_hist
    WHERE record_inserted_by = 'seed-data'
      AND lower(issue_changelog_itemsfield) IN ('status', 'epic status')
""").collect()])
