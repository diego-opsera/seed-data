# Diagnostic round 1: value-stream / Issue Stream feature
#
# Goal: confirm starting state before creating user_working schema + table.
# Specifically:
#   1. Does the playground_prod.user_working schema already exist?
#   2. Does playground_prod.user_working.offerings_jira_pipeline_details exist?
#      If yes — schema, row count, sample rows.
#   3. Are there OTHER tables anywhere in playground_prod with a similar name
#      (offerings_jira_pipeline_details) that we might collide with or that
#      already hold the data we need?
#   4. Schemas already in playground_prod — confirms we're not stepping on
#      anything when we create user_working.
#
# Read-only. No writes. Run via:
#   exec(open("/tmp/seed-data/notebooks/value_stream/diag_1.py").read())

import json

CATALOG = "playground_prod"


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


def ddl(table):
    return sql(f"SHOW CREATE TABLE {table}").collect()[0][0]


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def safe(label, fn):
    try:
        out(label, fn())
    except Exception as e:
        out(label, {"error": str(e)})


# ── 1. Schemas in playground_prod ────────────────────────────────────────────

out(
    "catalog.schemas",
    [r["databaseName"] for r in sql(f"SHOW SCHEMAS IN {CATALOG}").collect()],
)

safe(
    "user_working.exists",
    lambda: rows(
        f"SHOW SCHEMAS IN {CATALOG} LIKE 'user_working'"
    ),
)

# ── 2. Target table existence ────────────────────────────────────────────────

TARGET = f"{CATALOG}.user_working.offerings_jira_pipeline_details"

safe("target.tables_in_user_working",
     lambda: [r.asDict() for r in sql(f"SHOW TABLES IN {CATALOG}.user_working").collect()])

safe("target.schema",   lambda: schema(TARGET))
safe("target.ddl",      lambda: ddl(TARGET))
safe("target.row_count", lambda: sql(f"SELECT COUNT(*) AS n FROM {TARGET}").collect()[0]["n"])
safe("target.sample_5", lambda: rows(f"SELECT * FROM {TARGET}", 5))

# Demo-org row counts — only meaningful if target exists.
safe(
    "target.rows_by_org",
    lambda: rows(
        f"""
        SELECT org_name, COUNT(*) AS n
        FROM {TARGET}
        GROUP BY org_name
        ORDER BY n DESC
        """,
        50,
    ),
)

safe(
    "target.demo_org_rows",
    lambda: sql(
        f"""
        SELECT COUNT(*) AS n
        FROM {TARGET}
        WHERE org_name IN ('demo-acme-direct', 'demo-meridian')
        """
    ).collect()[0]["n"],
)

# ── 3. Look for any other table with this name anywhere in playground_prod ──
# (so we don't accidentally duplicate something that already exists in another schema)

safe(
    "catalog.tables_named_offerings_jira_pipeline_details",
    lambda: [
        r.asDict()
        for r in sql(
            f"""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM {CATALOG}.information_schema.tables
            WHERE table_name = 'offerings_jira_pipeline_details'
            """
        ).collect()
    ],
)

# Broader: anything whose name even contains 'offerings' or 'pipeline_details'
safe(
    "catalog.tables_matching_offerings_or_pipeline_details",
    lambda: [
        r.asDict()
        for r in sql(
            f"""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM {CATALOG}.information_schema.tables
            WHERE table_name ILIKE '%offerings%'
               OR table_name ILIKE '%pipeline_details%'
               OR table_name ILIKE '%jira_pipeline%'
            ORDER BY table_schema, table_name
            """
        ).collect()
    ],
)

# ── 4. Cross-check: filter_groups_unity values we'll align SBG/Offering to ──
# Confirms the level_1 / level_3 values we'll mirror as SBG / Offering.

FGU = f"{CATALOG}.master_data.filter_groups_unity"

safe(
    "fgu.demo_levels",
    lambda: rows(
        f"""
        SELECT DISTINCT level_1, level_2, level_3, createdBy
        FROM {FGU}
        WHERE createdBy IN ('seed-data@demo.io', 'seed-data-meridian@demo.io')
        ORDER BY level_1, level_3
        """,
        20,
    ),
)
