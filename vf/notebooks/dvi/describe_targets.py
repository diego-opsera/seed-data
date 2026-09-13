# Phase 2 prep — schemas + sample payloads for every table the vf/ generators
# will write to.
#
# Three of the targets store their payload as a JSON string in a single column
# (pull_requests / commit_details / message) and the ETL reads it with
# get_json_object. Knowing the paths it reads is not enough to write a generator
# — we need the full column list, the NOT NULL / partition columns, and one real
# payload per table to mirror.
#
# Also dumps the DDL of the two views our inserts surface through:
#   base_datasets.v_itsm_issues_hist          (Quality + Impact read this, we
#                                              insert into transform_stage)
#   base_datasets.v_github_teams_members_current  (team labels; see BUGS.md #2
#                                              global-MAX trap)
#
# Output is compact JSON, not Spark .show() — no ASCII table borders to paste back.
#
# READ-ONLY: DESCRIBE / SHOW CREATE TABLE / SELECT with LIMIT only.
#
# Run:
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/describe_targets.py").read())

import json

CATALOG = "playground_prod"
JSON_SAMPLE_CHARS = 900    # hard cap — a single raw PR payload is ~18KB


def sql(q):
    return spark.sql(q)


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


def err(label, e):
    print(f"\n### {label}")
    print(json.dumps({"error": str(e).splitlines()[0][:300]}, indent=2))


def schema(table):
    try:
        return {
            r["col_name"]: r["data_type"]
            for r in sql(f"DESCRIBE {table}").collect()
            if r["col_name"] and not r["col_name"].startswith("#")
        }
    except Exception as e:
        return {"error": str(e).splitlines()[0][:300]}


def rows(q, limit=5):
    try:
        return [r.asDict() for r in sql(q).limit(limit).collect()]
    except Exception as e:
        return {"error": str(e).splitlines()[0][:300]}


def ddl(table):
    try:
        return sql(f"SHOW CREATE TABLE {table}").collect()[0][0]
    except Exception as e:
        return {"error": str(e).splitlines()[0][:300]}


def json_payload(table, col, where=""):
    """One raw payload from a JSON-string column, truncated."""
    try:
        r = sql(f"SELECT {col} AS payload FROM {table} {where} LIMIT 1").collect()
        if not r or r[0]["payload"] is None:
            return None
        raw = str(r[0]["payload"])
        return {
            "total_chars": len(raw),
            "truncated": len(raw) > JSON_SAMPLE_CHARS,
            "payload": raw[:JSON_SAMPLE_CHARS],
        }
    except Exception as e:
        return {"error": str(e).splitlines()[0][:300]}


def json_keys(table, col):
    """Top-level keys of a JSON-string column, so nesting is obvious."""
    try:
        r = sql(f"""
            SELECT json_object_keys({col}) AS keys FROM {table}
            WHERE {col} IS NOT NULL LIMIT 1
        """).collect()
        return r[0]["keys"] if r else None
    except Exception as e:
        return {"error": str(e).splitlines()[0][:300]}


print("## Phase 2 target schemas —", CATALOG)

# ── schemas available ───────────────────────────────────────────────────────
out("catalog.schemas", [r[0] for r in sql(f"SHOW SCHEMAS IN {CATALOG}").collect()])

# ── 1. PRs (JSON: pull_requests) ────────────────────────────────────────────
T = f"{CATALOG}.source_to_stage.raw_github_pull_requests_rest_api_prs"
out("prs.schema", schema(T))
out("prs.row_count", rows(f"SELECT COUNT(*) AS n FROM {T}", 1))
out("prs.non_json_columns_sample", rows(f"""
    SELECT * EXCEPT (pull_requests) FROM {T}
    ORDER BY record_insert_datetime DESC
""", 3))
out("prs.payload_keys", json_keys(T, "pull_requests"))
out("prs.payload_sample", json_payload(T, "pull_requests"))
out("prs.ddl", ddl(T))

T = f"{CATALOG}.source_to_stage.raw_github_pull_requests_rest_api_prs_details"
out("prs_details.schema", schema(T))
# EXCEPT the blob — selecting `details` through rows() emits ~20KB per row.
out("prs_details.non_json_sample", rows(f"SELECT * EXCEPT (details) FROM {T}", 3))
out("prs_details.payload_keys", json_keys(T, "details"))
out("prs_details.payload_sample", json_payload(T, "details"))

# ── 2. Commits (JSON: commit_details) ───────────────────────────────────────
T = f"{CATALOG}.source_to_stage.raw_github_commits_rest_api"
out("commits_raw.schema", schema(T))
out("commits_raw.row_count", rows(f"SELECT COUNT(*) AS n FROM {T}", 1))
out("commits_raw.non_json_columns_sample", rows(f"SELECT * EXCEPT (commit_details) FROM {T}", 3))
out("commits_raw.payload_keys", json_keys(T, "commit_details"))
out("commits_raw.payload_sample", json_payload(T, "commit_details"))
out("commits_raw.ddl", ddl(T))

# ── 3. GitHub Actions runs (JSON: message) — Security tier 3 + deploys ──────
T = f"{CATALOG}.source_to_stage.github_actions_runs_rest_api"
out("gha_runs.schema", schema(T))
out("gha_runs.row_count", rows(f"SELECT COUNT(*) AS n FROM {T}", 1))
out("gha_runs.non_json_columns_sample", rows(f"SELECT * EXCEPT (message) FROM {T}", 3))
out("gha_runs.payload_keys", json_keys(T, "message"))
out("gha_runs.payload_sample", json_payload(T, "message"))
out("gha_runs.workflow_names", rows(f"""
    SELECT get_json_object(message, '$.name') AS workflow_name, COUNT(*) AS n
    FROM {T} GROUP BY 1 ORDER BY n DESC
""", 25))
out("gha_runs.ddl", ddl(T))

# ── 4. ITSM — we insert into transform_stage, ETL reads the base_datasets view ─
T = f"{CATALOG}.transform_stage.mt_itsm_issues_current"
out("itsm_current.schema", schema(T))
out("itsm_current.row_count_by_customer", rows(f"""
    SELECT customer_id, COUNT(*) AS n FROM {T} GROUP BY 1 ORDER BY n DESC
""", 15))
out("itsm_current.dvi_columns_sample", rows(f"""
    SELECT itsm_source, issue_key, issue_type, issue_status, issue_priority,
           assignee_name, assignee_email, story_points,
           issue_created_date, issue_updated_date, issue_resolution_date,
           customer_id
    FROM {T}
""", 2))

T = f"{CATALOG}.transform_stage.mt_itsm_issues_hist"
out("itsm_hist.schema", schema(T))
out("itsm_hist.row_count", rows(f"SELECT COUNT(*) AS n FROM {T}", 1))

# CRITICAL: how do our transform_stage inserts reach the view the ETL reads?
out("v_itsm_issues_hist.ddl", ddl(f"{CATALOG}.base_datasets.v_itsm_issues_hist"))
out("v_itsm_issues_current.ddl", ddl(f"{CATALOG}.base_datasets.v_itsm_issues_current"))

# ── 5. Sonar — Security tier 2 + per-developer quality ──────────────────────
T = f"{CATALOG}.source_to_stage.raw_sonar_metric_split_data_branchwise"
out("sonar_metric.schema", schema(T))
out("sonar_metric.sample", rows(f"SELECT * FROM {T} WHERE org_name = 'demo-acme-direct'", 2))

T = f"{CATALOG}.source_to_stage.raw_sonar_type_data_branchwise"
out("sonar_type.schema", schema(T))
out("sonar_type.sample", rows(f"SELECT * FROM {T}", 2))

# ── 6. Team membership — individuals[].team labels ──────────────────────────
T = f"{CATALOG}.source_to_stage.raw_github_teams_members"
out("teams_members.schema", schema(T))
out("teams_members.sample", rows(f"SELECT * FROM {T}", 3))
# BUGS.md #2 — global MAX(record_update_datetime) pattern can hide other orgs.
out("v_github_teams_members_current.ddl",
    ddl(f"{CATALOG}.base_datasets.v_github_teams_members_current"))

# ── 7. base_datasets fallbacks we already seed ──────────────────────────────
# commits_rest_api is where BUGS.md #12 lives — commit_email is never populated.
T = f"{CATALOG}.base_datasets.commits_rest_api"
out("commits_bd.schema", schema(T))
out("commits_bd.email_coverage_by_org", rows(f"""
    SELECT org_name,
           COUNT(*) AS rows,
           SUM(CASE WHEN commit_email IS NULL OR trim(commit_email) = '' THEN 1 ELSE 0 END) AS null_email
    FROM {T} GROUP BY 1 ORDER BY rows DESC
""", 15))
out("commits_bd.sample_demo_acme_direct", rows(f"""
    SELECT * FROM {T} WHERE org_name = 'demo-acme-direct'
""", 1))

T = f"{CATALOG}.base_datasets.pull_requests"
out("prs_bd.schema", schema(T))
out("prs_bd.sample_demo_acme_direct", rows(f"""
    SELECT * FROM {T} WHERE org_name = 'demo-acme-direct'
""", 1))

print("\n## done — paste the above back to drive the Phase 2 generators")
