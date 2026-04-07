# CTFC round 14: verify data exists in v_itsm_issues_hist and simulate real chart join
# Chart uses v_itsm_issues_hist (not _current), filters by issue_changelog_itemsfield='status'
# and issue_updated BETWEEN start_date AND end_date
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_14.py").read())

import json

CATALOG  = "playground_prod"
CTFC_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"
LEVEL_3  = "demo-acme-corp"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def count(q): return sql(q).collect()[0][0]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. Does v_itsm_issues_hist exist and have our seed data? ─────────────────
out("v_itsm_issues_hist.seed_count", [{"count": count(f"""
    SELECT COUNT(*) FROM {CATALOG}.base_datasets.v_itsm_issues_hist
    WHERE record_inserted_by = 'seed-data'
""")}])

# ── 2. Sample from v_itsm_issues_hist for our data ────────────────────────────
out("v_itsm_issues_hist.seed_sample", rows(f"""
    SELECT issue_key, issue_status, issue_created, issue_updated,
           issue_changelog_itemsfield, issue_project,
           transform(board_info, x -> CAST(x.board_id AS STRING)) AS board_ids
    FROM {CATALOG}.base_datasets.v_itsm_issues_hist
    WHERE record_inserted_by = 'seed-data'
    LIMIT 2
""", 2))

# ── 3. Underlying table for v_itsm_issues_hist ────────────────────────────────
out("v_itsm_issues_hist.ddl", [{"ddl": r[0]} for r in sql(f"""
    SHOW CREATE TABLE {CATALOG}.base_datasets.v_itsm_issues_hist
""").collect()])

# ── 4. Does mt_itsm_issues_hist have our data? ────────────────────────────────
out("mt_itsm_issues_hist.seed_count", [{"count": count(f"""
    SELECT COUNT(*) FROM {CATALOG}.transform_stage.mt_itsm_issues_hist
    WHERE record_inserted_by = 'seed-data'
""")}])

# ── 5. Simulate real chart join using v_itsm_issues_hist ─────────────────────
out("ctfc.real_chart_sim", rows(f"""
    WITH source AS (
        SELECT issue_key, issue_status, issue_created,
               issue_updated AS resolved_date,
               issue_project, issue_type, issue_changelog_itemsfield,
               transform(board_info, x -> CAST(x.board_id AS STRING)) AS board_ids
        FROM {CATALOG}.base_datasets.v_itsm_issues_hist
        WHERE record_inserted_by = 'seed-data'
          AND lower(issue_changelog_itemsfield) IN ('status', 'epic status')
    ),
    filter_groups AS (
        SELECT DISTINCT board_ids, issue_status, include_issue_types, project_name
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
    )
    SELECT s.issue_key, s.issue_status, s.issue_project, s.issue_type,
           s.issue_created, s.resolved_date,
           datediff(s.resolved_date, s.issue_created) AS cycle_days
    FROM source s
    JOIN filter_groups f
      ON (array_contains(f.project_name, s.issue_project) OR f.project_name IS NULL)
     AND (arrays_overlap(f.board_ids, s.board_ids) OR f.board_ids IS NULL)
     AND array_contains(f.issue_status, s.issue_status)
     AND IF(f.include_issue_types IS NOT NULL, array_contains(f.include_issue_types, s.issue_type), TRUE)
""", 3))
