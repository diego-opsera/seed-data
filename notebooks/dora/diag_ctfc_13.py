# CTFC round 13: find jira_boards underlying table and its schema
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_ctfc_13.py").read())

import json

CATALOG = "playground_prod"

def sql(q): return spark.sql(q)
def out(label, data): print(f"\n### {label}"); print(json.dumps(data, default=str, indent=2))

# ── 1. View definition for jira_boards ───────────────────────────────────────
out("jira_boards.ddl", [{"ddl": r[0]} for r in sql(f"""
    SHOW CREATE TABLE {CATALOG}.base_datasets.jira_boards
""").collect()])

# ── 2. raw_jira_boards_ci schema ─────────────────────────────────────────────
out("raw_jira_boards_ci.schema", [r.asDict() for r in sql(f"""
    DESCRIBE {CATALOG}.source_to_stage.raw_jira_boards_ci
""").collect()])

# ── 3. raw_jira_boards_ci sample ─────────────────────────────────────────────
out("raw_jira_boards_ci.sample", [r.asDict() for r in sql(f"""
    SELECT * FROM {CATALOG}.source_to_stage.raw_jira_boards_ci LIMIT 1
""").collect()])
