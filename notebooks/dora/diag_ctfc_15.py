# CTFC round 15: compare mt_itsm_issues_hist schema vs current, sample real hist rows
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_15.py").read())

import json

CATALOG = "playground_prod"

def sql(q): return spark.sql(q)
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. mt_itsm_issues_hist schema ────────────────────────────────────────────
out("mt_itsm_issues_hist.schema", [r.asDict() for r in sql(f"""
    DESCRIBE {CATALOG}.transform_stage.mt_itsm_issues_hist
""").collect()])

# ── 2. Sample real (non-seed) hist row ────────────────────────────────────────
out("mt_itsm_issues_hist.real_sample", [r.asDict() for r in sql(f"""
    SELECT * FROM {CATALOG}.transform_stage.mt_itsm_issues_hist
    WHERE record_inserted_by != 'seed-data'
    LIMIT 1
""").collect()])

# ── 3. How many rows per issue_key in real hist? (to understand row multiplicity)
out("mt_itsm_issues_hist.rows_per_issue", [r.asDict() for r in sql(f"""
    SELECT issue_key, COUNT(*) AS n
    FROM {CATALOG}.transform_stage.mt_itsm_issues_hist
    WHERE record_inserted_by != 'seed-data'
    GROUP BY issue_key
    ORDER BY n DESC
    LIMIT 5
""").collect()])
