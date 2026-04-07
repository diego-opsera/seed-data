# Diagnostic round 5: schemas for pipeline_activities, cfr_mttr_metric_data, filter_values_unity
# Goal: get full column lists so we can build DORA chart data generators
# Run via exec(open("/tmp/seed-data/notebooks/dora/diag_dora_5.py").read())

import json

CATALOG = "playground_prod"

def sql(q):
    return spark.sql(q)

def rows(q, limit=5):
    return [r.asDict() for r in sql(q).limit(limit).collect()]

def schema(table):
    return {r["col_name"]: r["data_type"] for r in sql(f"DESCRIBE {table}").collect() if r["col_name"] and not r["col_name"].startswith("#")}

def ddl(table):
    return sql(f"SHOW CREATE TABLE {table}").collect()[0][0]

def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))

# ── pipeline_activities ───────────────────────────────────────────────────────

PA = f"{CATALOG}.base_datasets.pipeline_activities"

out("pa.schema", schema(PA))
out("pa.row_count", sql(f"SELECT COUNT(*) AS n FROM {PA}").collect()[0]["n"])
out("pa.sample_3_rows", rows(f"SELECT * FROM {PA}", 3))
out("pa.ddl", ddl(PA))

# distinct values for key joining/scoping columns
out("pa.distinct_org_name", rows(f"""
    SELECT org_name, COUNT(*) AS n FROM {PA}
    GROUP BY org_name ORDER BY n DESC
""", 20))

# ── cfr_mttr_metric_data ──────────────────────────────────────────────────────

CFR = f"{CATALOG}.base_datasets.cfr_mttr_metric_data"

out("cfr.schema", schema(CFR))
out("cfr.row_count", sql(f"SELECT COUNT(*) AS n FROM {CFR}").collect()[0]["n"])
out("cfr.sample_3_rows", rows(f"SELECT * FROM {CFR}", 3))
out("cfr.ddl", ddl(CFR))

out("cfr.distinct_issue_project", rows(f"""
    SELECT issue_project, COUNT(*) AS n FROM {CFR}
    GROUP BY issue_project ORDER BY n DESC
""", 20))

# ── filter_values_unity ───────────────────────────────────────────────────────

FVU = f"{CATALOG}.master_data.filter_values_unity"

out("fvu.schema", schema(FVU))
out("fvu.row_count", sql(f"SELECT COUNT(*) AS n FROM {FVU}").collect()[0]["n"])
out("fvu.sample_3_rows", rows(f"SELECT * FROM {FVU}", 3))
out("fvu.ddl", ddl(FVU))

# find a row that has board_ids or project_name set — these are the DORA-relevant filter types
out("fvu.dora_relevant_rows", rows(f"""
    SELECT * FROM {FVU}
    WHERE filter_name IN ('board_ids', 'project_name', 'fix_version', 'project_url', 'pipeline_tags', 'jql_filter_ids')
    LIMIT 5
""", 5))
