# Phase 0 / step 3 — replay the DVI ETL's own dimension logic and predict the tiles.
#
# syncDvi.js resolves each dimension through a fallback chain and then the
# frontend normalizes the raw value against fixed thresholds. This script runs
# the same resolution against playground_prod and prints what each of the five
# tiles WOULD show — without waiting on an ETL run.
#
# Faithful to visualforge@b8377ed22:
#   - snapshot dimension values      etl/syncDvi.js:1233-1292
#   - security 3-tier fallback       etl/syncDvi.js:1110-1126
#   - quality via ITSM leakage       etl/syncDvi.js:1245-1268
#   - normalizeScore + weights       frontend/src/dashboard/dvi/dviCompute.ts
#                                    frontend/src/dashboard/dvi/dviConfig.ts
#
# READ-ONLY. No INSERT / UPDATE / DELETE / CREATE.
#
# Run:
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/diag_dimensions.py").read())

from datetime import date

CATALOG = "playground_prod"

_today = date.today()
_y, _m = _today.year, _today.month - 12
while _m <= 0:
    _m += 12
    _y -= 1
FROM_DATE = date(_y, _m, min(_today.day, 28)).isoformat()
TO_DATE = _today.isoformat()

DONE_STATUSES = (
    "'done','closed','resolved','completed','complete','released',"
    "'ready for production','ready for release','approved for deploy',"
    "'product deployment','fixed'"
)
FEATURE_TYPES = (
    "'story','feature','new feature','enhancement','epic','user story',"
    "'product backlog item'"
)
DEFECT_PRIORITIES = "'BLOCKER','CRITICAL','HIGHEST','1','1 - CRITICAL'"

# dviConfig.ts DEFAULT_DVI_CONFIG — thresholds the tiles normalize against.
CONFIG = {
    "velocity":   {"w": 0.25, "inv": True,  "exc": 4,  "good": 8,  "ni": 24, "unit": "hours",
                   "metric": "PR Cycle Time"},
    "quality":    {"w": 0.25, "inv": True,  "exc": 5,  "good": 15, "ni": 25, "unit": "%",
                   "metric": "Defect leakage to production"},
    "security":   {"w": 0.20, "inv": False, "exc": 95, "good": 85, "ni": 70, "unit": "%",
                   "metric": "Security Scan Pass Rate"},
    "throughput": {"w": 0.15, "inv": False, "exc": 20, "good": 15, "ni": 8,  "unit": "merged PRs",
                   "metric": "Story points completed / sprint (actually merged PRs)"},
    "impact":     {"w": 0.15, "inv": False, "exc": 12, "good": 8,  "ni": 3,  "unit": "count",
                   "metric": "Features shipped to production"},
}


def hr(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def scalar(sql, default=None):
    """Return first column of first row; None-safe; never raises."""
    try:
        rows = spark.sql(sql).collect()
        if not rows:
            return default
        v = rows[0][0]
        return default if v is None else v
    except Exception as e:
        print(f"  QUERY FAILED — {str(e).splitlines()[0][:180]}")
        return default


def row(sql):
    try:
        rows = spark.sql(sql).collect()
        return rows[0].asDict() if rows else {}
    except Exception as e:
        print(f"  QUERY FAILED — {str(e).splitlines()[0][:180]}")
        return {}


def table_ok(fqn):
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1").collect()
        return True
    except Exception:
        return False


def _interp(x, x0, x1, y0, y1):
    if x1 == x0:
        return y0
    t = (x - x0) / (x1 - x0)
    return max(0.0, min(100.0, y0 + t * (y1 - y0)))


def normalize_score(raw, c):
    """Port of dviCompute.ts normalizeScore — piecewise-linear against thresholds."""
    exc, good, ni = c["exc"], c["good"], c["ni"]
    if c["inv"]:
        if raw <= exc:
            return 100.0
        if raw <= good:
            return _interp(raw, exc, good, 100, 75)
        if raw <= ni:
            return _interp(raw, good, ni, 75, 50)
        decay_ceiling = ni + (ni - exc)
        if raw >= decay_ceiling:
            return 0.0
        return _interp(raw, ni, decay_ceiling, 50, 0)
    if raw >= exc:
        return 100.0
    if raw >= good:
        return _interp(raw, good, exc, 75, 100)
    if raw >= ni:
        return _interp(raw, ni, good, 50, 75)
    if raw <= 0:
        return 0.0
    return _interp(raw, 0, ni, 0, 50)


print(f"Replaying syncDvi(months=12): {FROM_DATE} → {TO_DATE}")

# ─────────────────────────────────────────────────────────────────────────────
hr("1. Velocity + Throughput — merged PR union, bucketed by created_at")
print("The ETL merges ALL PR sources (dedup on pr_id:repository), then the SNAPSHOT reads only")
print("the latest month that has PRs. Building the same union here.\n")

PR_SOURCES = {
    "raw-github-rest": f"""
        SELECT CAST(get_json_object(pull_requests, '$.id') AS STRING) AS pr_id,
               get_json_object(pull_requests, '$.base.repo.full_name')  AS repository,
               get_json_object(pull_requests, '$.created_at')           AS created_at,
               get_json_object(pull_requests, '$.merged_at')            AS merged_at
        FROM {CATALOG}.source_to_stage.raw_github_pull_requests_rest_api_prs
    """,
    "base-datasets": f"""
        SELECT CAST(merge_request_id AS STRING) AS pr_id,
               COALESCE(CAST(project_url AS STRING), CAST(project_name AS STRING)) AS repository,
               CAST(COALESCE(pr_created_datetime, pr_created_date) AS STRING) AS created_at,
               CAST(pr_merged_datetime AS STRING) AS merged_at
        FROM {CATALOG}.base_datasets.pull_requests
        WHERE merge_request_id IS NOT NULL
    """,
    "cl-supplement": f"""
        SELECT CAST(pr_id AS STRING) AS pr_id,
               CAST(project_name AS STRING) AS repository,
               CAST(COALESCE(pr_created_datetime, pr_created_date) AS STRING) AS created_at,
               CAST(COALESCE(pr_merged_datetime, pr_merged_date)   AS STRING) AS merged_at
        FROM {CATALOG}.consumption_layer.commits_prs
        WHERE pr_id IS NOT NULL
          AND github_commit_email IS NOT NULL
          AND lower(trim(github_commit_email)) LIKE '%@%'
    """,
}

live = []
for label, sql in PR_SOURCES.items():
    fqn = sql.split("FROM ")[1].split()[0]
    if table_ok(fqn):
        live.append(sql)
        print(f"  usable: {label}")
    else:
        print(f"  UNREADABLE (absent or no permission): {label}")

velocity_raw = 0.0
throughput_raw = 0
latest_month = None

if live:
    union = "\n  UNION ALL\n".join(f"  ({s})" for s in live)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW vf_dvi_pr_union AS
        WITH raw AS (
        {union}
        ),
        deduped AS (
          SELECT pr_id, repository, created_at, merged_at,
                 ROW_NUMBER() OVER (PARTITION BY pr_id, repository ORDER BY merged_at DESC NULLS LAST) AS rn
          FROM raw
          WHERE pr_id IS NOT NULL AND created_at IS NOT NULL
        )
        SELECT pr_id, repository, created_at, merged_at
        FROM deduped WHERE rn = 1
    """)

    print("\n-- per-month, deduped (this is what prByMonth looks like) --")
    spark.sql(f"""
        SELECT date_format(to_date(created_at), 'yyyy-MM') AS created_month,
               COUNT(*) AS total_prs,
               SUM(CASE WHEN merged_at IS NOT NULL THEN 1 ELSE 0 END) AS merged_prs,
               ROUND(percentile_approx(
                 CASE WHEN merged_at IS NOT NULL THEN
                   (unix_timestamp(to_timestamp(merged_at)) - unix_timestamp(to_timestamp(created_at))) / 3600.0
                 END, 0.5), 2) AS median_cycle_hours
        FROM vf_dvi_pr_union
        WHERE to_date(created_at) >= '{FROM_DATE}' AND to_date(created_at) <= '{TO_DATE}'
        GROUP BY 1 ORDER BY 1
    """).show(60, truncate=False)

    latest = row(f"""
        SELECT date_format(to_date(created_at), 'yyyy-MM') AS mk,
               SUM(CASE WHEN merged_at IS NOT NULL THEN 1 ELSE 0 END) AS merged_prs,
               ROUND(percentile_approx(
                 CASE WHEN merged_at IS NOT NULL THEN
                   (unix_timestamp(to_timestamp(merged_at)) - unix_timestamp(to_timestamp(created_at))) / 3600.0
                 END, 0.5), 2) AS median_cycle_hours
        FROM vf_dvi_pr_union
        WHERE to_date(created_at) >= '{FROM_DATE}' AND to_date(created_at) <= '{TO_DATE}'
        GROUP BY 1
        HAVING SUM(CASE WHEN merged_at IS NOT NULL THEN 1 ELSE 0 END) > 0
        ORDER BY mk DESC LIMIT 1
    """)
    if latest:
        latest_month = latest.get("mk")
        throughput_raw = int(latest.get("merged_prs") or 0)
        velocity_raw = float(latest.get("median_cycle_hours") or 0.0)

print(f"\n  latest month with merged PRs : {latest_month or 'NONE'}")
print(f"  velocity_raw   (median hours): {velocity_raw}")
print(f"  throughput_raw (merged PRs)  : {throughput_raw}")
if latest_month and latest_month != _today.strftime("%Y-%m"):
    print(f"  WARNING: latest PR month is {latest_month}, not the current month "
          f"({_today.strftime('%Y-%m')}).")
    print("           The snapshot falls back one month only; older than that reads as 0.")

# ─────────────────────────────────────────────────────────────────────────────
hr("2. Quality — ITSM defect leakage (no org filter in the ETL)")
qual = row(f"""
    WITH issues AS (
      SELECT issue_key, issue_type, issue_priority,
             ROW_NUMBER() OVER (PARTITION BY issue_key ORDER BY issue_updated_date DESC) AS rn
      FROM {CATALOG}.base_datasets.v_itsm_issues_hist
      WHERE issue_created_date IS NOT NULL
        AND to_date(issue_created_date) >= '{FROM_DATE}'
        AND to_date(issue_created_date) <= '{TO_DATE}'
    )
    SELECT COUNT(DISTINCT issue_key) AS total_issues,
           COUNT(DISTINCT CASE WHEN lower(trim(issue_type)) IN ('bug','defect')
                                AND upper(trim(issue_priority)) IN ({DEFECT_PRIORITIES})
                               THEN issue_key END) AS high_priority_defects
    FROM issues WHERE rn = 1
""")
itsm_total = int(qual.get("total_issues") or 0)
itsm_defects = int(qual.get("high_priority_defects") or 0)
quality_available = itsm_total > 0
quality_raw = round(itsm_defects / itsm_total * 100, 2) if quality_available else 0.0
print(f"  total_issues          : {itsm_total}")
print(f"  high_priority_defects : {itsm_defects}")
print(f"  qualitySource         : {'jira/ado' if quality_available else 'none  → tile renders N/A'}")
print(f"  defect leakage        : {quality_raw}%")
if quality_available and itsm_defects == 0:
    print("  NOTE: issues exist but none match the required priority set, so leakage is 0% —")
    print("        the tile reads as PERFECT quality rather than a realistic number.")

# ─────────────────────────────────────────────────────────────────────────────
hr("3. Security — three-tier fallback")
t1 = row(f"""
    SELECT SUM(CASE WHEN upper(type) = 'VULNERABILITY' THEN 1 ELSE 0 END) AS total_vulns,
           SUM(CASE WHEN upper(type) = 'VULNERABILITY'
                     AND upper(status) IN ('RESOLVED','CLOSED') THEN 1 ELSE 0 END) AS resolved_vulns
    FROM {CATALOG}.base_datasets.asp_sonar_issues
""")
t1_total = int(t1.get("total_vulns") or 0)
t1_resolved = int(t1.get("resolved_vulns") or 0)

t2 = row(f"""
    SELECT COUNT(*) AS total_gates,
           SUM(CASE WHEN quality_gate_status = 'OK' THEN 1 ELSE 0 END) AS passed_gates
    FROM {CATALOG}.source_to_stage.raw_sonar_metric_split_data_branchwise
    WHERE last_analysis_date IS NOT NULL
      AND to_date(last_analysis_date) >= '{FROM_DATE}'
      AND to_date(last_analysis_date) <= '{TO_DATE}'
""")
t2_total = int(t2.get("total_gates") or 0)
t2_passed = int(t2.get("passed_gates") or 0)

t3 = row(f"""
    WITH parsed AS (
      SELECT get_json_object(message, '$.name') AS workflow_name,
             lower(get_json_object(message, '$.conclusion')) AS conclusion
      FROM {CATALOG}.source_to_stage.github_actions_runs_rest_api
      WHERE record_insert_datetime >= '{FROM_DATE}'
        AND record_insert_datetime <= '{TO_DATE}'
        AND get_json_object(message, '$.conclusion') IS NOT NULL
    ),
    flagged AS (
      SELECT conclusion,
             CASE WHEN lower(workflow_name) LIKE '%security%' OR lower(workflow_name) LIKE '%scan%'
                    OR lower(workflow_name) LIKE '%codeql%'   OR lower(workflow_name) LIKE '%sast%'
                    OR lower(workflow_name) LIKE '%dast%'     OR lower(workflow_name) LIKE '%vulnerability%'
                    OR lower(workflow_name) LIKE '%snyk%'     OR lower(workflow_name) LIKE '%trivy%'
                    OR lower(workflow_name) LIKE '%dependabot%' OR lower(workflow_name) LIKE '%quality%'
                    OR lower(workflow_name) LIKE '%lint%'     OR lower(workflow_name) LIKE '%test%'
                  THEN 1 ELSE 0 END AS is_sec
      FROM parsed
    )
    SELECT SUM(is_sec) AS security_workflow_runs,
           SUM(CASE WHEN is_sec = 1 AND conclusion = 'success' THEN 1 ELSE 0 END) AS security_success_runs
    FROM flagged
""")
t3_total = int(t3.get("security_workflow_runs") or 0)
t3_success = int(t3.get("security_success_runs") or 0)

print(f"  tier 1  asp_sonar_issues                    : vulns={t1_total} resolved={t1_resolved}")
print(f"  tier 2  raw_sonar_metric_split_data_branchwise: gates={t2_total} passed={t2_passed}")
print(f"  tier 3  github_actions_runs_rest_api          : sec_runs={t3_total} success={t3_success}")

if t1_total > 0:
    security_source = "asp_sonar_issues"
    security_raw = round(t1_resolved / t1_total * 100, 2)
elif t2_total > 0:
    security_source = "sonarqube"
    security_raw = round(t2_passed / t2_total * 100, 2)
elif t3_total > 0:
    security_source = "gha"
    security_raw = round(t3_success / t3_total * 100, 2)
else:
    security_source = "none"
    security_raw = 0.0

security_available = security_source != "none"
print(f"\n  securitySource : {security_source}"
      f"{'  → tile renders N/A' if not security_available else ''}")
print(f"  pass rate      : {security_raw}%")
print("  NOTE: tier 1 wins on ANY vulnerability row, whole-table, no date or org filter — so a")
print("        handful of stale asp_sonar_issues rows can mask a fully-seeded tier 2.")

# ─────────────────────────────────────────────────────────────────────────────
hr("4. Impact — ITSM features shipped, else deploy count")
imp = row(f"""
    WITH resolved AS (
      SELECT issue_key, issue_type,
             ROW_NUMBER() OVER (PARTITION BY issue_key
                                ORDER BY issue_updated_date DESC, issue_resolution_date DESC) AS rn
      FROM {CATALOG}.base_datasets.v_itsm_issues_hist
      WHERE issue_resolution_date IS NOT NULL
        AND issue_updated_date >= '{FROM_DATE}'
        AND issue_updated_date <= '{TO_DATE}'
        AND lower(trim(issue_status)) IN ({DONE_STATUSES})
    )
    SELECT COUNT(DISTINCT CASE WHEN lower(trim(issue_type)) IN ({FEATURE_TYPES})
                               THEN issue_key END) AS feature_issues
    FROM resolved WHERE rn = 1
""")
feature_issues = int(imp.get("feature_issues") or 0)

deploys_latest = 0
if table_ok(f"{CATALOG}.source_to_stage.raw_mongo_pipelineactivities"):
    deploys_latest = int(scalar(f"""
        SELECT COUNT(DISTINCT _id)
        FROM {CATALOG}.source_to_stage.raw_mongo_pipelineactivities
        WHERE createdAt >= '{FROM_DATE}'
          AND date_format(to_date(createdAt), 'yyyy-MM') = '{latest_month or _today.strftime('%Y-%m')}'
    """, 0) or 0)

impact_raw = feature_issues if feature_issues > 0 else deploys_latest
print(f"  feature_issues (completed, window) : {feature_issues}")
print(f"  deploys in latest month (fallback) : {deploys_latest}")
print(f"  impact_raw                         : {impact_raw}")

# ─────────────────────────────────────────────────────────────────────────────
hr("5. Predicted tiles")
raws = {
    "velocity": velocity_raw,
    "quality": quality_raw,
    "security": security_raw,
    "throughput": float(throughput_raw),
    "impact": float(impact_raw),
}
avail = {
    "velocity": True,               # no availability flag is emitted for velocity
    "quality": quality_available,
    "security": security_available,
    "throughput": True,
    "impact": True,
}

print(f"{'dimension':<12} {'raw':>10} {'unit':<14} {'score':>7}  {'wt':>5}  source")
print("-" * 78)
score, total_w = 0.0, 0.0
for k, c in CONFIG.items():
    if avail[k]:
        n = round(normalize_score(raws[k], c), 1)
        score += n * c["w"]
        total_w += c["w"]
        shown = f"{n:>7.1f}"
    else:
        shown = f"{'N/A':>7}"
    print(f"{k:<12} {raws[k]:>10.2f} {c['unit']:<14} {shown}  {c['w']:>5.2f}  {c['metric']}")

if total_w > 0 and total_w < 1:
    score *= 1 / total_w      # computeDvi re-normalizes over available dimensions
print("-" * 78)
print(f"  predicted DVI score: {round(score, 1)} / 100")
print("\n  This is the SNAPSHOT path (individuals[] empty). Once per-developer rows exist the tiles")
print("  switch to avg(individuals[dim]) instead — see dviCompute.ts computeDviFromIndividuals.")

hr("Verdict")
gaps = []
if not latest_month:
    gaps.append("no merged PRs at all → Velocity + Throughput dead")
elif latest_month != _today.strftime("%Y-%m"):
    gaps.append(f"newest PR month is {latest_month}, not {_today.strftime('%Y-%m')}")
if not quality_available:
    gaps.append("no ITSM rows → Quality N/A")
elif itsm_defects == 0:
    gaps.append("ITSM priorities never match BLOCKER/CRITICAL/HIGHEST → leakage always 0%")
if not security_available:
    gaps.append("no security rows in any of the 3 tiers → Security N/A")
if impact_raw == 0:
    gaps.append("no completed feature issues and no deploys → Impact 0")
if gaps:
    print("  Gaps to close in Phase 2:")
    for g in gaps:
        print(f"    - {g}")
else:
    print("  All five dimensions resolve. If the UI still shows N/A, the gap is the ETL run")
    print("  (POST /etl/sync/dvi) or the mapping-group scope, not the Databricks data.")
