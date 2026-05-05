# Diagnostic round 3: investigate base_datasets.pipeline_activities for demo orgs
#
# Goal: understand the shape of dora-generated pipeline_activities so we can
# bridge it to the value-stream pipeline-failures feature via two new tables:
#   - user_working.repo_pipeline_details
#   - user_working.github_offering_workflow_job_logs
#
# pipeline-failures.sql joins pipeline_activities on pipeline_id + project_name
# AND requires step_conclusion='failure', step_id IS NOT NULL, and rpd's
# pipeline_started_at >= DATE_SUB(CURRENT_DATE(), 30).
#
# Read-only. Run via:
#   exec(open("/tmp/seed-data/notebooks/value_stream/diag_3.py").read())

import json

CATALOG = "playground_prod"
PA = f"{CATALOG}.base_datasets.pipeline_activities"


def sql(q):
    return spark.sql(q)


def rows(q, limit=20):
    return [r.asDict() for r in sql(q).limit(limit).collect()]


def schema(table):
    return {
        r["col_name"]: r["data_type"]
        for r in sql(f"DESCRIBE {table}").collect()
        if r["col_name"] and not r["col_name"].startswith("#")
    }


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def safe(label, fn):
    try:
        out(label, fn())
    except Exception as e:
        out(label, {"error": str(e).split(chr(10))[0]})


# ── 1. Schema of pipeline_activities ─────────────────────────────────────────

safe("pa.schema", lambda: schema(PA))

# ── 2. Demo-org row counts (record_inserted_by scope) ────────────────────────

DEMO_SCOPE = "record_inserted_by IN ('seed-data', 'seed-data-meridian')"

safe(
    "pa.demo_rows_total",
    lambda: sql(f"SELECT COUNT(*) AS n FROM {PA} WHERE {DEMO_SCOPE}").collect()[0]["n"],
)

safe(
    "pa.demo_rows_by_inserted_by",
    lambda: rows(
        f"""
        SELECT record_inserted_by, COUNT(*) AS n
        FROM {PA}
        WHERE {DEMO_SCOPE}
        GROUP BY record_inserted_by
        """
    ),
)

# ── 3. Failure rows + last-30-day filter ─────────────────────────────────────

safe(
    "pa.demo_failures_total",
    lambda: sql(
        f"""
        SELECT COUNT(*) AS n
        FROM {PA}
        WHERE {DEMO_SCOPE}
          AND step_conclusion = 'failure'
        """
    ).collect()[0]["n"],
)

safe(
    "pa.demo_failures_last_30d",
    lambda: sql(
        f"""
        SELECT COUNT(*) AS n
        FROM {PA}
        WHERE {DEMO_SCOPE}
          AND step_conclusion = 'failure'
          AND pipeline_started_at >= DATE_SUB(CURRENT_DATE(), 30)
        """
    ).collect()[0]["n"],
)

safe(
    "pa.demo_failures_last_30d_by_org",
    lambda: rows(
        f"""
        SELECT record_inserted_by, project_name, COUNT(*) AS n
        FROM {PA}
        WHERE {DEMO_SCOPE}
          AND step_conclusion = 'failure'
          AND pipeline_started_at >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY record_inserted_by, project_name
        ORDER BY n DESC
        """
    ),
)

# ── 4. Sample failure rows to understand column shapes ───────────────────────

safe(
    "pa.sample_failure_rows_3",
    lambda: rows(
        f"""
        SELECT *
        FROM {PA}
        WHERE {DEMO_SCOPE}
          AND step_conclusion = 'failure'
          AND pipeline_started_at >= DATE_SUB(CURRENT_DATE(), 30)
        ORDER BY pipeline_started_at DESC
        """,
        3,
    ),
)

# ── 5. Distinct values for fields we'll mirror into repo_pipeline_details ────

safe(
    "pa.distinct_pipeline_names_demo",
    lambda: rows(
        f"""
        SELECT DISTINCT pipeline_name, project_name, COUNT(*) AS n
        FROM {PA}
        WHERE {DEMO_SCOPE}
        GROUP BY pipeline_name, project_name
        ORDER BY n DESC
        """,
        20,
    ),
)

safe(
    "pa.step_id_type_check",
    lambda: rows(
        f"""
        SELECT step_id, CAST(step_id AS STRING) AS step_id_str, step_name
        FROM {PA}
        WHERE {DEMO_SCOPE}
          AND step_conclusion = 'failure'
        LIMIT 5
        """
    ),
)

# ── 6. Check if either of the target tables already exist ────────────────────

for tbl in ["repo_pipeline_details", "github_offering_workflow_job_logs"]:
    safe(
        f"target_{tbl}.exists_check",
        lambda t=tbl: rows(
            f"""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM {CATALOG}.information_schema.tables
            WHERE table_schema = 'user_working' AND table_name = '{t}'
            """
        ),
    )
