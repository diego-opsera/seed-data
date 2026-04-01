"""
Generator for consumption_layer.release_management_detail.

This table backs the entire Release Management dashboard. Each row represents
one (release, project, kpi_uuid) snapshot. The dashboard queries filter on
level_1 (enterprise name) and kpi_uuids; LATERAL VIEW explode unpacks the
ARRAY<STRING> columns which contain JSON-encoded objects.

Demo data: 4 releases × 2 projects × 4 kpi_uuid types = 32 rows.
Scoped by level_1 = enterprise["name"] (e.g. 'demo-acme-corp').
"""
import json
import random
from datetime import date, datetime, timedelta

TABLE = "release_management_detail"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.release_management_detail
  (fix_version, issue_project, start_date, release_date, release_status,
   level_name, level_value, level_1, kpi_uuids,
   issue_completion_details, pipeline_trigger_details, total_prs,
   defect_density_details, total_commits, approval_gates,
   vulnerabilities, bugs, webapp_vulnerabilities, tests)
VALUES
{values};"""

# KPI UUIDs that the dashboard filters on — derived from release-management.handler.js
_KPI_UUIDS = [
    "9fd5ec78-9fce-49a0-8154-24d3109d3f05",  # overview / commits / PRs / pipelines
    "f60d8a58-7c8d-4dd6-9b54-6c07715ae5ec",  # CTFC / do-ratio
    "cebb8ee5-8229-4e29-a2f3-a4875adf21fe",  # defect density
    "60aed2f8-1c74-4792-ad51-bf4e5a65f7b9",  # builds / approval gates / tests / vulns
]

# (fix_version, project, start_date, release_date, status)
_RELEASES = [
    ("v2025.1", "ACME-BACKEND",  date(2025, 1,  6), date(2025, 1, 31), "released"),
    ("v2025.2", "ACME-BACKEND",  date(2025, 4,  1), date(2025, 4, 30), "released"),
    ("v2025.3", "ACME-BACKEND",  date(2025, 7,  1), date(2025, 7, 31), "released"),
    ("v2025.4", "ACME-BACKEND",  date(2025, 10, 1), date(2025, 10, 31), "unreleased"),
    ("v2025.1", "ACME-FRONTEND", date(2025, 1,  6), date(2025, 1, 31), "released"),
    ("v2025.2", "ACME-FRONTEND", date(2025, 4,  1), date(2025, 4, 30), "released"),
    ("v2025.3", "ACME-FRONTEND", date(2025, 7,  1), date(2025, 7, 31), "released"),
    ("v2025.4", "ACME-FRONTEND", date(2025, 10, 1), date(2025, 10, 31), "unreleased"),
]

_AUTHORS = [
    ("demo-alice", "alice@acme.com"),
    ("demo-bob",   "bob@acme.com"),
    ("demo-carol", "carol@acme.com"),
]

_ENVS = ["DEV", "QA", "UAT", "PREPROD", "PROD"]


def _sql_array(items: list) -> str:
    """Serialize a list of dicts to a Databricks ARRAY<STRING> literal."""
    parts = []
    for item in items:
        encoded = json.dumps(item, default=str).replace("'", "''")
        parts.append(f"'{encoded}'")
    return f"array({', '.join(parts)})"


def _sha(rng: random.Random) -> str:
    return f"{rng.randint(0, 0xFFFFFF):06x}{rng.randint(0, 0xFFFFFF):06x}"


def _ts(dt: date, rng: random.Random) -> str:
    t = timedelta(hours=rng.randint(8, 20), minutes=rng.randint(0, 59))
    return (datetime.combine(dt, datetime.min.time()) + t).strftime("%Y-%m-%dT%H:%M:%S")


def _make_issues(rng, project, n_done, n_total, start_dt, release_dt):
    issues = []
    base = rng.randint(100, 200)
    span = max((release_dt - start_dt).days, 1)
    for i in range(n_total):
        is_done = i < n_done
        updated_dt = start_dt + timedelta(days=rng.randint(1, span))
        cycle_secs = rng.randint(3_600, 7 * 24 * 3_600)
        issues.append({
            "issue_key":         f"{project}-{base + i}",
            "issue_project_key": project,
            "issue_type":        rng.choice(["Story", "Task", "Bug"]),
            "issue_summary":     f"Demo issue {base + i}",
            "issue_status":      "Done" if is_done else rng.choice(["In Progress", "In Review"]),
            "issue_priority":    rng.choice(["High", "Medium", "Low"]),
            "sprint_name":       f"Sprint {rng.randint(1, 12)}",
            "sprint_state":      "closed" if is_done else "active",
            "assignee_name":     rng.choice([a[0] for a in _AUTHORS]),
            "story_points":      str(rng.choice([1, 2, 3, 5, 8])),
            "is_done":           str(is_done).lower(),
            "is_moved":          "false",
            "current_time":      cycle_secs if is_done else None,
            "issue_created":     (start_dt - timedelta(days=rng.randint(5, 15))).strftime("%Y-%m-%d"),
            "issue_updated":     updated_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "issue_link":        f"https://acme.atlassian.net/browse/{project}-{base + i}",
        })
    return issues


def _make_defects(rng, issues, defect_ratio=0.15):
    return [
        {
            "issue_key":    iss["issue_key"],
            "issue_type":   "Bug" if rng.random() < defect_ratio else iss["issue_type"],
            "issue_updated": iss["issue_updated"],
            "isdefect":     str(rng.random() < defect_ratio).lower(),
        }
        for iss in issues
    ]


def _make_pipelines(rng, project, fix_version, n, release_dt):
    pipelines = []
    for i in range(n):
        env = _ENVS[i % len(_ENVS)]
        start_dt = release_dt - timedelta(days=rng.randint(1, 10))
        start_ts = datetime.combine(start_dt, datetime.min.time()) + timedelta(hours=rng.randint(8, 20))
        end_ts = start_ts + timedelta(seconds=rng.randint(120, 900))
        pipelines.append({
            "pipeline_id":         f"pipeline-{rng.randint(1000, 9999)}",
            "pipeline_name":       f"{project.lower()}-{env.lower()}-deploy",
            "pipeline_group":      env,
            "pipeline_status":     "success" if i < n - 1 else rng.choice(["success", "failed"]),
            "pipeline_run_count":  str(i + 1),
            "pipeline_started_at": start_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "pipeline_finished_at": end_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "pipeline_url":        f"https://app.opsera.io/pipelines/{rng.randint(10_000, 99_999)}",
            "pipeline_commit_sha": _sha(rng),
            "issue_key":           f"{project}-{rng.randint(100, 200)}",
            "issue_project":       project,
            "issue_status":        "In Progress",
            "issue_summary":       f"Deploy {fix_version}",
            "project_name":        f"{project.lower()}-repo",
            "branch":              "main",
            "commit_message":      f"chore: bump version for {fix_version}",
            "commit_author":       rng.choice([a[0] for a in _AUTHORS]),
            "ltfc_seconds":        rng.randint(3_600, 5 * 24 * 3_600),
        })
    return pipelines


def _make_prs(rng, project, n, start_dt, release_dt):
    prs = []
    span = max((release_dt - start_dt).days - 2, 1)
    for _ in range(n):
        created_dt = start_dt + timedelta(days=rng.randint(0, span))
        review_start = datetime.combine(created_dt, datetime.min.time()) + timedelta(hours=rng.randint(8, 20))
        review_end = review_start + timedelta(hours=rng.randint(2, 72))
        merged = review_end + timedelta(hours=rng.randint(1, 24))
        authors = [a[0] for a in _AUTHORS]
        prs.append({
            "merge_request_id":                   str(rng.randint(1000, 9999)),
            "pr_author":                          rng.choice(authors),
            "latest_pr_review_user":              rng.choice(authors),
            "pr_title":                           f"feat: {project.lower()} update",
            "pr_link":                            f"https://github.com/demo-acme/{project.lower()}/pull/{rng.randint(100, 999)}",
            "source_branch":                      f"feature/{project.lower()}-{rng.randint(1, 50)}",
            "target_branch":                      "main",
            "project_name":                       f"{project.lower()}-repo",
            "pr_created_datetime":                review_start.strftime("%Y-%m-%dT%H:%M:%S"),
            "latest_pr_review_submitted_datetime": review_end.strftime("%Y-%m-%dT%H:%M:%S"),
            "pr_merged_datetime":                 merged.strftime("%Y-%m-%dT%H:%M:%S"),
            "requested_reviewers":                json.dumps(rng.sample(authors, 2)),
            "pr_reviewers":                       json.dumps(rng.sample(authors, 1)),
            "pr_tickets":                         f"{project}-{rng.randint(100, 200)}",
            "pr_state":                           "merged",
            "merge_status":                       "true",
            "latest_pr_review_state":             "approved",
        })
    return prs


def _make_commits(rng, project, fix_version, n, release_dt):
    commits = []
    for i in range(n):
        ts = datetime.combine(release_dt - timedelta(days=rng.randint(1, 20)), datetime.min.time()) + timedelta(hours=rng.randint(8, 20))
        author = _AUTHORS[i % len(_AUTHORS)]
        commits.append({
            "commit_id":        _sha(rng),
            "commit_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "commit_tickets":   f"{project}-{rng.randint(100, 200)}",
            "project_name":     f"{project.lower()}-repo",
            "web_url":          f"https://github.com/demo-acme/{project.lower()}/commit/{_sha(rng)}",
            "branch":           "main",
            "commit_title":     f"chore: update {project.lower()} for {fix_version}",
            "commit_author":    author[0],
            "commit_email":     author[1],
            "is_copilot_user":  str(rng.random() < 0.65).lower(),
        })
    return commits


def _make_gates(rng, project, n, release_dt):
    gates = []
    tools = ["sonarqube", "checkmarx", "jenkins"]
    for i in range(n):
        started = datetime.combine(release_dt - timedelta(days=rng.randint(1, 5)), datetime.min.time()) + timedelta(hours=rng.randint(9, 17))
        finished = started + timedelta(seconds=rng.randint(60, 600))
        tool = tools[i % len(tools)]
        gates.append({
            "project_name":   f"{project.lower()}-repo",
            "pipeline_id":    f"pipeline-{rng.randint(1000, 9999)}",
            "tool_identifier": f"{tool}-1",
            "step_id":        f"step-{i + 1}",
            "run_count":      str(rng.randint(1, 5)),
            "step_started_at": started.strftime("%Y-%m-%dT%H:%M:%S"),
            "step_finished_at": finished.strftime("%Y-%m-%dT%H:%M:%S"),
            "step_status":    "success" if rng.random() > 0.1 else "failed",
            "tool_type":      tool,
        })
    return gates


def _make_vulns(rng, n, release_dt):
    vulns = []
    for _ in range(n):
        d1 = (release_dt - timedelta(days=rng.randint(10, 30))).strftime("%Y-%m-%d")
        d2 = (release_dt - timedelta(days=rng.randint(0, 9))).strftime("%Y-%m-%d")
        resolved = rng.random() > 0.3
        vulns.append({
            "activity_date":       d1,
            "last_activity_date":  d2 if resolved else d1,
            "component_sha_id":    f"comp-{rng.randint(1000, 9999)}",
            "unique_sha_id":       f"vuln-{rng.randint(10_000, 99_999)}",
        })
    return vulns


def _make_bugs(rng, project, n, release_dt):
    bugs = []
    for i in range(n):
        created = (release_dt - timedelta(days=rng.randint(5, 20))).strftime("%Y-%m-%d")
        is_open = rng.random() > 0.4
        bugs.append({
            "key":            f"{project}-BUG-{rng.randint(100, 999)}",
            "project":        project,
            "message":        f"Bug {i + 1} in {project.lower()}",
            "component":      f"{project.lower()}/src/main",
            "line":           str(rng.randint(1, 500)),
            "status":         "OPEN" if is_open else "CLOSED",
            "creation_date":  created,
            "closed_date":    None if is_open else (release_dt - timedelta(days=rng.randint(1, 4))).strftime("%Y-%m-%d"),
            "tool_identifier": "sonarqube-1",
            "tool_type":      "sonarqube",
        })
    return bugs


def _make_webapp_vulns(rng, n):
    vulns = []
    for _ in range(n):
        is_present = rng.random() > 0.3
        vulns.append({
            "WebsiteId":   f"web-{rng.randint(1, 5)}",
            "IssueId":     f"wvuln-{rng.randint(1000, 9999)}",
            "IsPresent":   str(is_present).lower(),
            "IsAddressed": str(not is_present or rng.random() > 0.5).lower(),
        })
    return vulns


def _make_tests(rng):
    total = rng.randint(150, 500)
    failed = rng.randint(0, 10)
    skipped = rng.randint(0, 20)
    return [{
        "total_tests":   str(total),
        "failed_tests":  str(failed),
        "skipped_tests": str(skipped),
        "success_tests": str(total - failed - skipped),
        "tool_identifier": "jenkins-1",
    }]


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    rng = random.Random(42)
    enterprise_name = entities["enterprise"]["name"]
    statements = []

    for fix_version, project, start_dt, release_dt, status in _RELEASES:
        n_total = rng.randint(12, 25)
        n_done  = int(n_total * (0.90 if status == "released" else 0.60))

        issues    = _make_issues(rng, project, n_done, n_total, start_dt, release_dt)
        defects   = _make_defects(rng, issues)
        pipelines = _make_pipelines(rng, project, fix_version, 5, release_dt)
        prs       = _make_prs(rng, project, rng.randint(8, 15), start_dt, release_dt)
        commits   = _make_commits(rng, project, fix_version, rng.randint(20, 35), release_dt)
        gates     = _make_gates(rng, project, 3, release_dt)
        vulns     = _make_vulns(rng, rng.randint(3, 8), release_dt)
        bugs      = _make_bugs(rng, project, rng.randint(2, 6), release_dt)
        webapp    = _make_webapp_vulns(rng, rng.randint(2, 5))
        tests     = _make_tests(rng)

        for kpi_uuid in _KPI_UUIDS:
            row = (
                f"('{fix_version}', '{project}', "
                f"'{start_dt}', '{release_dt}', '{status}', "
                f"'enterprise', '{enterprise_name}', '{enterprise_name}', "
                f"'{kpi_uuid}', "
                f"{_sql_array(issues)}, "
                f"{_sql_array(pipelines)}, "
                f"{_sql_array(prs)}, "
                f"{_sql_array(defects)}, "
                f"{_sql_array(commits)}, "
                f"{_sql_array(gates)}, "
                f"{_sql_array(vulns)}, "
                f"{_sql_array(bugs)}, "
                f"{_sql_array(webapp)}, "
                f"{_sql_array(tests)})"
            )
            statements.append(INSERT_SQL.format(catalog=catalog, values=row))

    return statements
