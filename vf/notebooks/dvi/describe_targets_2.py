# Phase 2 prep, round 2 — the sections describe_targets.py truncated away.
#
# Round 1 blew the output limit because it dumped whole JSON blobs through
# rows() (a single raw_github_pull_requests_rest_api_prs payload is 18KB). This
# one never selects a JSON column via rows(): payloads come from json_payload()
# truncated hard, and row samples use * EXCEPT (<json col>).
#
# Still missing after round 1:
#   - source_to_stage.raw_github_commits_rest_api      (commit_details shape)
#   - source_to_stage.github_actions_runs_rest_api     (message shape, workflows)
#   - transform_stage.mt_itsm_issues_current / _hist   (our ITSM insert target)
#   - base_datasets.v_itsm_issues_hist DDL             (how our rows surface)
#   - master_data.github_copilot_orgs_mapping          (the teams view joins it)
#   - the global MAX(record_update_datetime) on raw_github_teams_members
#
# READ-ONLY. DESCRIBE / SHOW CREATE TABLE / SELECT ... LIMIT only.
#
# Run:
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/describe_targets_2.py").read())

import json

CATALOG = "playground_prod"
PAYLOAD_CHARS = 900     # hard cap — enough to see structure, small enough to paste


def sql(q):
    return spark.sql(q)


def out(label, data):
    print(f"\n### {label}")
    print(json.dumps(data, default=str, indent=2))


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


def ddl(obj):
    try:
        return sql(f"SHOW CREATE TABLE {obj}").collect()[0][0]
    except Exception as e:
        return {"error": str(e).splitlines()[0][:300]}


def json_payload(table, col, where=""):
    try:
        r = sql(f"SELECT {col} AS p FROM {table} {where} LIMIT 1").collect()
        if not r or r[0]["p"] is None:
            return None
        raw = str(r[0]["p"])
        return {"total_chars": len(raw), "head": raw[:PAYLOAD_CHARS]}
    except Exception as e:
        return {"error": str(e).splitlines()[0][:300]}


def json_keys(table, col):
    try:
        r = sql(f"SELECT json_object_keys({col}) AS k FROM {table} WHERE {col} IS NOT NULL LIMIT 1").collect()
        return r[0]["k"] if r else None
    except Exception as e:
        return {"error": str(e).splitlines()[0][:300]}


print("## Phase 2 target schemas, round 2 —", CATALOG)

# ── 1. Commits raw (JSON: commit_details) ───────────────────────────────────
T = f"{CATALOG}.source_to_stage.raw_github_commits_rest_api"
out("commits_raw.schema", schema(T))
out("commits_raw.row_count", rows(f"SELECT COUNT(*) AS n FROM {T}", 1))
out("commits_raw.non_json_sample", rows(f"SELECT * EXCEPT (commit_details) FROM {T}", 3))
out("commits_raw.payload_keys", json_keys(T, "commit_details"))
out("commits_raw.payload_head", json_payload(T, "commit_details"))
# The ETL reads $.commit.author.email — confirm it is actually present.
out("commits_raw.author_email_presence", rows(f"""
    SELECT org_name,
           COUNT(*) AS rows,
           SUM(CASE WHEN get_json_object(commit_details, '$.commit.author.email') IS NULL
                    THEN 1 ELSE 0 END) AS null_author_email,
           SUM(CASE WHEN get_json_object(commit_details, '$.stats.additions') IS NULL
                    THEN 1 ELSE 0 END) AS null_stats_additions
    FROM {T} GROUP BY 1 ORDER BY rows DESC
""", 12))

# ── 2. GitHub Actions runs (JSON: message) — Security tier 3 + deploys ──────
T = f"{CATALOG}.source_to_stage.github_actions_runs_rest_api"
out("gha_runs.schema", schema(T))
out("gha_runs.row_count", rows(f"SELECT COUNT(*) AS n FROM {T}", 1))
out("gha_runs.non_json_sample", rows(f"SELECT * EXCEPT (message) FROM {T}", 3))
out("gha_runs.payload_keys", json_keys(T, "message"))
out("gha_runs.payload_head", json_payload(T, "message"))
out("gha_runs.top_workflow_names", rows(f"""
    SELECT get_json_object(message, '$.name') AS workflow_name,
           lower(get_json_object(message, '$.conclusion')) AS conclusion,
           COUNT(*) AS n
    FROM {T} GROUP BY 1, 2 ORDER BY n DESC
""", 25))
out("gha_runs.repo_url_sample", rows(f"""
    SELECT repo_url, owner, COUNT(*) AS n,
           MIN(to_date(record_insert_datetime)) AS min_insert,
           MAX(to_date(record_insert_datetime)) AS max_insert
    FROM {T} GROUP BY 1, 2 ORDER BY n DESC
""", 12))
out("gha_runs.ddl", ddl(T))

# ── 3. ITSM — we insert into transform_stage, ETL reads the base_datasets view ─
T = f"{CATALOG}.transform_stage.mt_itsm_issues_current"
out("itsm_current.schema", schema(T))
out("itsm_current.count_by_customer", rows(f"""
    SELECT customer_id, itsm_source, COUNT(*) AS n
    FROM {T} GROUP BY 1, 2 ORDER BY n DESC
""", 15))
# Only the columns the DVI dimensions actually read — avoids dumping board_info
# / filter_info / labels blobs.
out("itsm_current.dvi_columns_sample", rows(f"""
    SELECT itsm_source, issue_key, issue_type, issue_status, issue_priority,
           assignee_name, assignee_email, story_points,
           issue_created_date, issue_updated_date, issue_resolution_date,
           issue_project, issue_project_key, customer_id, record_insert_datetime
    FROM {T} WHERE customer_id = 'demo-acme-direct'
""", 3))

T = f"{CATALOG}.transform_stage.mt_itsm_issues_hist"
out("itsm_hist.schema", schema(T))
out("itsm_hist.count_by_customer", rows(f"""
    SELECT customer_id, COUNT(*) AS n FROM {T} GROUP BY 1 ORDER BY n DESC
""", 15))

# THE critical question: how do transform_stage rows reach the view the ETL reads?
out("v_itsm_issues_hist.ddl", ddl(f"{CATALOG}.base_datasets.v_itsm_issues_hist"))
out("v_itsm_issues_current.ddl", ddl(f"{CATALOG}.base_datasets.v_itsm_issues_current"))

# ── 4. Team labels — the view needs an org-mapping row, not just members ────
# v_github_teams_members_current joins master_data.github_copilot_orgs_mapping
# and explodes linked_org_name, then filters
#   record_update_datetime IN (SELECT MAX(record_update_datetime) FROM ...)
# so demo-acme-vf needs BOTH a mapping row and a record_update_datetime equal to
# the existing global max (setting it higher would hide every other org —
# BUGS.md #2).
T = f"{CATALOG}.master_data.github_copilot_orgs_mapping"
out("orgs_mapping.schema", schema(T))
out("orgs_mapping.rows", rows(f"SELECT * FROM {T}", 20))

T = f"{CATALOG}.source_to_stage.raw_github_teams_members"
out("teams_members.global_max_record_update_datetime", rows(f"""
    SELECT MAX(record_update_datetime) AS global_max,
           COUNT(*) AS total_rows,
           SUM(CASE WHEN record_update_datetime IS NULL THEN 1 ELSE 0 END) AS null_update_dt
    FROM {T}
""", 1))
out("teams_members.rows_at_global_max_by_org", rows(f"""
    SELECT org_name, record_update_datetime, COUNT(*) AS n
    FROM {T}
    WHERE record_update_datetime IN (SELECT MAX(record_update_datetime) FROM {T})
    GROUP BY 1, 2 ORDER BY n DESC
""", 15))
out("teams_members.count_by_org_all", rows(f"""
    SELECT org_name, COUNT(*) AS n,
           MIN(record_update_datetime) AS min_upd,
           MAX(record_update_datetime) AS max_upd
    FROM {T} GROUP BY 1 ORDER BY n DESC
""", 15))

# ── 5. Sonar metric table — schema only (round 1 already showed samples) ───
out("sonar_metric.schema", schema(f"{CATALOG}.source_to_stage.raw_sonar_metric_split_data_branchwise"))

print("\n## done")
