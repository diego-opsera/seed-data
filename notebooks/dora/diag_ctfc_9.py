# CTFC round 9: inspect v_itsm_issues_current and jira_boards schemas + samples
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_9.py").read())

import json

CATALOG  = "playground_prod"
LEVEL_3  = "demo-acme-corp"

def sql(q): return spark.sql(q)
def rows(q, n=5): return [r.asDict() for r in sql(q).limit(n).collect()]
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. v_itsm_issues_current schema ───────────────────────────────────────────
out("v_itsm_issues_current.schema", [r.asDict() for r in sql(f"""
    DESCRIBE {CATALOG}.base_datasets.v_itsm_issues_current
""").collect()])

# ── 2. Sample row from v_itsm_issues_current ──────────────────────────────────
out("v_itsm_issues_current.sample", rows(f"""
    SELECT * FROM {CATALOG}.base_datasets.v_itsm_issues_current LIMIT 1
""", 1))

# ── 3. jira_boards schema ─────────────────────────────────────────────────────
out("jira_boards.schema", [r.asDict() for r in sql(f"""
    DESCRIBE {CATALOG}.base_datasets.jira_boards
""").collect()])

# ── 4. Sample row from jira_boards ────────────────────────────────────────────
out("jira_boards.sample", rows(f"""
    SELECT * FROM {CATALOG}.base_datasets.jira_boards LIMIT 1
""", 1))

# ── 5. mt_itsm_issues_current schema (underlying table) ──────────────────────
out("mt_itsm_issues_current.schema", [r.asDict() for r in sql(f"""
    DESCRIBE {CATALOG}.transform_stage.mt_itsm_issues_current
""").collect()])

# ── 6. Sample mt_itsm_issues_current row ──────────────────────────────────────
out("mt_itsm_issues_current.sample", rows(f"""
    SELECT * FROM {CATALOG}.transform_stage.mt_itsm_issues_current LIMIT 1
""", 1))
