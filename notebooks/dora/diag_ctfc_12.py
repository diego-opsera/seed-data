# CTFC round 12: check jira_boards for board_id=1, and test full join with jira_boards
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_12.py").read())

import json

CATALOG  = "playground_prod"
CTFC_KPI = "0cb8981e-3a17-4a14-b1f3-83bcecf10373"
LEVEL_3  = "demo-acme-corp"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def count(q): return sql(q).collect()[0][0]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. Does board_id=1 exist in jira_boards? ──────────────────────────────────
out("jira_boards.board_1", rows(f"""
    SELECT * FROM {CATALOG}.base_datasets.jira_boards
    WHERE board_id = 1
""", 5))

# ── 2. What board_ids exist in jira_boards? (first 10) ───────────────────────
out("jira_boards.all_ids", [r.asDict() for r in sql(f"""
    SELECT board_id, board_name FROM {CATALOG}.base_datasets.jira_boards
    ORDER BY board_id LIMIT 10
""").collect()])

# ── 3. Simulate join WITH jira_boards ────────────────────────────────────────
out("ctfc.sim_with_jira_boards", rows(f"""
    SELECT e.issue_key, e.issue_status, e.issue_type,
           e.issue_created, e.issue_resolution_date,
           jb.board_id, jb.board_name,
           datediff(e.issue_resolution_date, e.issue_created) AS cycle_days
    FROM (
        SELECT i.*, bi
        FROM {CATALOG}.base_datasets.v_itsm_issues_current i
        LATERAL VIEW EXPLODE(i.board_info) t AS bi
    ) e
    JOIN {CATALOG}.base_datasets.jira_boards jb
      ON e.bi.board_id = jb.board_id
    JOIN (
        SELECT project_name, issue_status, board_ids, include_issue_types
        FROM {CATALOG}.master_data.v_filter_group_values_kpi_flattened_unity
        WHERE level_3 = '{LEVEL_3}' AND kpi_uuids = '{CTFC_KPI}'
        LIMIT 1
    ) f ON array_contains(f.project_name, e.issue_project)
       AND array_contains(f.issue_status, e.issue_status)
       AND array_contains(f.board_ids, CAST(jb.board_id AS STRING))
""", 3))

# ── 4. Check what issue_types our data has vs what filter expects ──────────────
out("itsm.our_issue_types", [r.asDict() for r in sql(f"""
    SELECT DISTINCT issue_type FROM {CATALOG}.base_datasets.v_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
""").collect()])

# ── 5. Check source_record_insert_date range ──────────────────────────────────
out("itsm.insert_date_range", [r.asDict() for r in sql(f"""
    SELECT MIN(source_record_insert_date) AS min_insert,
           MAX(source_record_insert_date) AS max_insert
    FROM {CATALOG}.base_datasets.v_itsm_issues_current
    WHERE record_inserted_by = 'seed-data'
""").collect()])
