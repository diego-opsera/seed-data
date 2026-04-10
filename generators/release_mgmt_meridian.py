"""
generators/release_mgmt_meridian.py

Generates INSERT statements for consumption_layer.release_management_detail
scoped to the Meridian Analytics data team.

Story arc (synced with dora_meridian.py inflection ~Oct 2025):

  Pre-Opsera (quarterly batch promotions):
    meridian-v2025.Q2  Apr 01 → Jun 27 2025   "released"
    meridian-v2025.Q3  Jul 01 → Sep 26 2025   "released"
    — Long LTFC (150+ days), high defect density, few PRs, manual pipelines

  Post-Opsera (Opsera-driven continuous releases):
    meridian-v2025.Q4  Oct 06 → Nov 28 2025   "released"
    meridian-v2026.Q1  Dec 01 → Feb 27 2026   "released"
    meridian-v2026.Q1.1 Mar 02 → Apr 10 2026  "unreleased"
    — Short LTFC (days), low defect density, many PRs, automated pipelines

Numbers are calibrated to look consistent with:
  - MDP-PROM-XXXX JIRA issues (dora_meridian)
  - commits at 1-2/deploy pre-Opsera, 2-4/user/day post-Opsera (devex_meridian)
  - PRs near-zero pre-Opsera, ramping post-Opsera (devex_meridian)
  - ServiceNow CRs: Emergency/Normal pre-Opsera, Standard post-Opsera (cr_meridian)

Deletion scoped via fix_version LIKE 'meridian-%'.
"""
import json
import random
from datetime import date, datetime, timedelta

TABLE = "release_management_detail"

INSERT_SQL = """\
INSERT INTO {catalog}.consumption_layer.release_management_detail
  (level_name, level_value, level_1,
   fix_version, issue_project,
   user_start_date, user_release_date,
   start_date, release_date, release_status,
   issue_completion_details, pipeline_trigger_details, total_prs,
   defect_density_details, total_commits, total_builds, approval_gates,
   vulnerabilities, bugs, webapp_vulnerabilities, tests)
VALUES
{values};"""

_LEVEL_VALUE = "Meridian Analytics"   # must match hierarchy filter "Project" in the dashboard
_PROJECT     = "MDP"                   # JIRA project key used inside issue JSON

# Releases aligned to dora_meridian maintenance windows and inflection
_RELEASES = [
    # (fix_version,            start_dt,            release_dt,           status,       phase)
    ("meridian-v2025.Q2",  date(2025, 4,  1), date(2025, 6, 27), "released",   "pre"),
    ("meridian-v2025.Q3",  date(2025, 7,  1), date(2025, 9, 26), "released",   "pre"),
    ("meridian-v2025.Q4",  date(2025, 10, 6), date(2025, 11, 28), "released",  "post"),
    ("meridian-v2026.Q1",  date(2025, 12, 1), date(2026, 2, 27), "released",   "post"),
    ("meridian-v2026.Q1.1",date(2026, 3,  2), date(2026, 4, 10), "unreleased", "post"),
]

_AUTHORS = [
    ("meridian-alice", "alice@demo-meridian.io"),
    ("meridian-bob",   "bob@demo-meridian.io"),
    ("meridian-carol", "carol@demo-meridian.io"),
]

_REPO_URL = "https://github.com/demo-meridian/data-platform"
_JIRA_URL = "https://meridian.atlassian.net/browse"

_ENVS = ["DEV", "QA", "STAGING", "PROD"]

# Pre-Opsera summaries: big-batch data platform promotions
_SUMMARIES_PRE = [
    "Promote data ingestion pipeline to production",
    "Deploy batch ETL refactor to production workspace",
    "Release transformation layer changes to production",
    "Promote data quality framework update",
    "Deploy schema migration to production Databricks",
    "Release PySpark job optimisations to production",
    "Promote data model changes: dimension tables",
    "Deploy orchestration updates to production",
    "Release data lineage tracking to production",
    "Promote monitoring and alerting configuration",
]

# Post-Opsera summaries: smaller, automated PR-based deployments
_SUMMARIES_POST = [
    "feat: update ingestion pipeline via Opsera",
    "fix: schema validation in transformation layer",
    "feat: add new data quality checks",
    "chore: update cluster config via Opsera pipeline",
    "feat: improve PySpark job performance",
    "fix: correct date partitioning in ETL",
    "feat: add monitoring dashboard metrics",
    "chore: dependency bump for data-platform",
]


def _sql_array(items: list) -> str:
    parts = []
    for item in items:
        encoded = json.dumps(item, default=str).replace("'", "''")
        parts.append(f"'{encoded}'")
    return f"array({', '.join(parts)})"


def _sha(rng: random.Random) -> str:
    return f"{rng.randint(0, 0xFFFFFF):06x}{rng.randint(0, 0xFFFFFF):06x}"


def _make_issues(rng, fix_version, phase, n_done, n_total, start_dt, release_dt):
    issues = []
    span = max((release_dt - start_dt).days, 1)
    base = rng.randint(100, 300)
    summaries = _SUMMARIES_PRE if phase == "pre" else _SUMMARIES_POST
    for i in range(n_total):
        is_done = i < n_done
        updated_dt = start_dt + timedelta(days=rng.randint(1, span))
        # Pre-Opsera: very long cycle times (weeks to months). Post: days.
        if phase == "pre":
            cycle_secs = rng.randint(15 * 24 * 3600, 90 * 24 * 3600)
        else:
            cycle_secs = rng.randint(1 * 24 * 3600, 7 * 24 * 3600)
        issues.append({
            "issue_key":         f"MDP-{base + i}",
            "issue_project_key": _PROJECT,
            "issue_type":        rng.choice(["Story", "Task", "Bug"]),
            "issue_summary":     rng.choice(summaries),
            "issue_status":      "Done" if is_done else rng.choice(["In Progress", "In Review"]),
            "issue_priority":    "High" if phase == "pre" else rng.choice(["High", "Medium", "Low"]),
            "sprint_name":       f"Sprint {rng.randint(1, 12)}",
            "sprint_state":      "closed" if is_done else "active",
            "assignee_name":     rng.choice([a[0] for a in _AUTHORS]),
            "story_points":      str(rng.choice([1, 2, 3, 5, 8])),
            "is_done":           str(is_done).lower(),
            "is_moved":          str(rng.random() < (0.20 if phase == "pre" else 0.05)).lower(),
            "current_time":      cycle_secs if is_done else None,
            "issue_created":     (start_dt - timedelta(days=rng.randint(5, 30))).strftime("%Y-%m-%d"),
            "issue_updated":     updated_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "issue_link":        f"{_JIRA_URL}/MDP-{base + i}",
        })
    return issues


def _make_defects(rng, issues, defect_ratio):
    return [
        {
            "issue_key":     iss["issue_key"],
            "issue_type":    "Bug" if rng.random() < defect_ratio else iss["issue_type"],
            "issue_updated": iss["issue_updated"],
            "isdefect":      str(rng.random() < defect_ratio).lower(),
        }
        for iss in issues
    ]


def _make_pipelines(rng, fix_version, phase, n, release_dt):
    pipelines = []
    # Pre-Opsera: manual pipelines, more failures, long runs
    # Post-Opsera: Opsera-driven, mostly success, faster
    for i in range(n):
        env = _ENVS[i % len(_ENVS)]
        start_dt = release_dt - timedelta(days=rng.randint(1, 10))
        start_ts = datetime.combine(start_dt, datetime.min.time()) + timedelta(hours=rng.randint(8, 20))
        duration = rng.randint(600, 3600) if phase == "pre" else rng.randint(120, 900)
        end_ts = start_ts + timedelta(seconds=duration)
        # Pre-Opsera: last pipeline has higher failure chance
        if phase == "pre":
            success = rng.random() > 0.25
        else:
            success = rng.random() > 0.08
        pipelines.append({
            "pipeline_id":          f"pipeline-{rng.randint(1000, 9999)}",
            "pipeline_name":        f"mdp-{env.lower()}-deploy",
            "pipeline_group":       env,
            "pipeline_status":      "success" if success else "failed",
            "pipeline_run_count":   str(i + 1),
            "pipeline_started_at":  start_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "pipeline_finished_at": end_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "pipeline_url":         f"https://app.opsera.io/pipelines/{rng.randint(10_000, 99_999)}",
            "pipeline_commit_sha":  _sha(rng),
            "issue_key":            f"MDP-{rng.randint(100, 400)}",
            "issue_project":        _PROJECT,
            "issue_status":         "In Progress",
            "issue_summary":        f"Deploy {fix_version}",
            "project_name":         "data-platform",
            "branch":               "main",
            "commit_message":       f"chore: promote {fix_version} to production",
            "commit_author":        rng.choice([a[0] for a in _AUTHORS]),
            "ltfc_seconds":         rng.randint(50 * 24 * 3600, 150 * 24 * 3600) if phase == "pre"
                                    else rng.randint(1 * 24 * 3600, 7 * 24 * 3600),
        })
    return pipelines


def _make_prs(rng, fix_version, phase, n, start_dt, release_dt):
    prs = []
    span = max((release_dt - start_dt).days - 2, 1)
    for _ in range(n):
        created_dt = start_dt + timedelta(days=rng.randint(0, span))
        review_start = datetime.combine(created_dt, datetime.min.time()) + timedelta(hours=rng.randint(8, 20))
        # Pre-Opsera: slow review (days). Post: fast review (hours).
        review_hours = rng.randint(48, 168) if phase == "pre" else rng.randint(2, 24)
        review_end = review_start + timedelta(hours=review_hours)
        merged = review_end + timedelta(hours=rng.randint(1, 4))
        authors = [a[0] for a in _AUTHORS]
        prs.append({
            "merge_request_id":                    str(rng.randint(1000, 9999)),
            "pr_author":                           rng.choice(authors),
            "latest_pr_review_user":               rng.choice(authors),
            "pr_title":                            f"feat: data-platform update ({fix_version})",
            "pr_link":                             f"{_REPO_URL}/pull/{rng.randint(1, 500)}",
            "source_branch":                       f"feature/mdp-{rng.randint(1, 100)}",
            "target_branch":                       "main",
            "project_name":                        "data-platform",
            "pr_created_datetime":                 review_start.strftime("%Y-%m-%dT%H:%M:%S"),
            "latest_pr_review_submitted_datetime": review_end.strftime("%Y-%m-%dT%H:%M:%S"),
            "pr_merged_datetime":                  merged.strftime("%Y-%m-%dT%H:%M:%S"),
            "requested_reviewers":                 rng.sample(authors, min(2, len(authors))),
            "pr_reviewers":                        rng.sample(authors, 1),
            "pr_tickets":                          f"MDP-{rng.randint(100, 400)}",
            "pr_state":                            "merged",
            "merge_status":                        "true",
            "latest_pr_review_state":              "approved",
        })
    return prs


def _make_commits(rng, fix_version, phase, n, start_dt, release_dt):
    commits = []
    span = max((release_dt - start_dt).days, 1)
    for i in range(n):
        ts = (datetime.combine(start_dt + timedelta(days=rng.randint(0, span)), datetime.min.time())
              + timedelta(hours=rng.randint(8, 20)))
        author = _AUTHORS[i % len(_AUTHORS)]
        # Copilot usage: very low pre-Opsera, high post-Opsera
        copilot_rate = 0.08 if phase == "pre" else 0.60
        commits.append({
            "commit_id":        _sha(rng),
            "commit_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "commit_tickets":   f"MDP-{rng.randint(100, 400)}",
            "project_name":     "data-platform",
            "web_url":          f"{_REPO_URL}/commit/{_sha(rng)}",
            "branch":           "main",
            "commit_title":     f"feat: promote data pipeline ({fix_version})" if phase == "pre"
                                else f"feat: data-platform update",
            "commit_author":    author[0],
            "commit_email":     author[1],
            "is_copilot_user":  str(rng.random() < copilot_rate).lower(),
        })
    return commits


def _make_builds(rng, phase, n, release_dt):
    builds = []
    for i in range(n):
        started = (datetime.combine(release_dt - timedelta(days=rng.randint(1, 7)), datetime.min.time())
                   + timedelta(hours=rng.randint(8, 20)))
        duration = rng.randint(300, 1800) if phase == "pre" else rng.randint(60, 600)
        finished = started + timedelta(seconds=duration)
        fail_rate = 0.20 if phase == "pre" else 0.05
        builds.append({
            "build_id":          f"build-{rng.randint(1000, 9999)}",
            "build_name":        f"data-platform-build-{i + 1}",
            "build_status":      "success" if rng.random() > fail_rate else "failed",
            "build_started_at":  started.strftime("%Y-%m-%dT%H:%M:%S"),
            "build_finished_at": finished.strftime("%Y-%m-%dT%H:%M:%S"),
            "project_name":      "data-platform",
            "branch":            "main",
        })
    return builds


def _make_gates(rng, phase, n, release_dt):
    gates = []
    # Pre-Opsera: manual gates only. Post: automated sonarqube + checkov + jenkins
    tools = ["sonarqube", "jenkins"] if phase == "pre" else ["sonarqube", "checkov", "jenkins"]
    for i in range(n):
        started = (datetime.combine(release_dt - timedelta(days=rng.randint(1, 5)), datetime.min.time())
                   + timedelta(hours=rng.randint(9, 17)))
        finished = started + timedelta(seconds=rng.randint(60, 600))
        tool = tools[i % len(tools)]
        gates.append({
            "project_name":    "data-platform",
            "pipeline_id":     f"pipeline-{rng.randint(1000, 9999)}",
            "tool_identifier": f"{tool}-1",
            "step_id":         f"step-{i + 1}",
            "run_count":       str(rng.randint(1, 3)),
            "step_started_at": started.strftime("%Y-%m-%dT%H:%M:%S"),
            "step_finished_at": finished.strftime("%Y-%m-%dT%H:%M:%S"),
            "step_status":     "success" if rng.random() > (0.20 if phase == "pre" else 0.05) else "failed",
            "tool_type":       tool,
        })
    return gates


def _make_vulns(rng, phase, n, release_dt):
    vulns = []
    for _ in range(n):
        d1 = (release_dt - timedelta(days=rng.randint(10, 30))).strftime("%Y-%m-%d")
        d2 = (release_dt - timedelta(days=rng.randint(0, 9))).strftime("%Y-%m-%d")
        # Pre-Opsera: more unresolved vulns. Post: mostly resolved.
        resolved = rng.random() > (0.50 if phase == "pre" else 0.15)
        vulns.append({
            "activity_date":      d1,
            "last_activity_date": d2 if resolved else d1,
            "component_sha_id":   f"comp-{rng.randint(1000, 9999)}",
            "unique_sha_id":      f"vuln-{rng.randint(10_000, 99_999)}",
        })
    return vulns


def _make_bugs(rng, phase, n, release_dt):
    bugs = []
    for i in range(n):
        created = (release_dt - timedelta(days=rng.randint(5, 30))).strftime("%Y-%m-%d")
        is_open = rng.random() > (0.30 if phase == "pre" else 0.60)
        bugs.append({
            "key":             f"MDP-BUG-{rng.randint(100, 999)}",
            "project":         _PROJECT,
            "message":         f"Data pipeline issue {i + 1}",
            "component":       "data-platform/src/pipelines",
            "line":            str(rng.randint(1, 500)),
            "status":          "OPEN" if is_open else "CLOSED",
            "creation_date":   created,
            "closed_date":     None if is_open else (release_dt - timedelta(days=rng.randint(1, 4))).strftime("%Y-%m-%d"),
            "tool_identifier": "sonarqube-1",
            "tool_type":       "sonarqube",
        })
    return bugs


def _make_webapp_vulns(rng, phase, n):
    vulns = []
    for _ in range(n):
        is_present = rng.random() > (0.20 if phase == "pre" else 0.60)
        vulns.append({
            "WebsiteId":   f"web-{rng.randint(1, 3)}",
            "IssueId":     f"wvuln-{rng.randint(1000, 9999)}",
            "IsPresent":   str(is_present).lower(),
            "IsAddressed": str(not is_present or rng.random() > (0.40 if phase == "pre" else 0.10)).lower(),
        })
    return vulns


def _make_tests(rng, phase):
    # Pre-Opsera: fewer tests (data team didn't have CI). Post: comprehensive test suite.
    total = rng.randint(30, 80) if phase == "pre" else rng.randint(150, 400)
    fail_ceiling = 15 if phase == "pre" else 5
    failed  = rng.randint(0, fail_ceiling)
    skipped = rng.randint(0, 10)
    return [{
        "total_tests":     str(total),
        "failed_tests":    str(failed),
        "skipped_tests":   str(skipped),
        "success_tests":   str(max(0, total - failed - skipped)),
        "tool_identifier": "jenkins-1",
    }]


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    rng = random.Random(137)   # distinct seed from Acme's rng=42
    statements = []

    for fix_version, start_dt, release_dt, status, phase in _RELEASES:
        # Issue counts: pre-Opsera = large batch, post-Opsera = small increment
        if phase == "pre":
            n_total = rng.randint(20, 30)
            n_done  = int(n_total * (0.88 if status == "released" else 0.60))
            n_pipelines = rng.randint(3, 5)
            n_prs       = rng.randint(2, 5)       # barely using PRs
            n_commits   = rng.randint(15, 28)     # 2 commits × 8-15 deploys
            n_builds    = rng.randint(3, 6)
            n_gates     = 2
            n_vulns     = rng.randint(6, 12)      # more vulns pre-Opsera
            n_bugs      = rng.randint(5, 10)
            n_webapp    = rng.randint(3, 6)
            defect_ratio = 0.25
        else:
            n_total = rng.randint(8, 15)
            n_done  = int(n_total * (0.93 if status == "released" else 0.65))
            n_pipelines = rng.randint(5, 8)
            n_prs       = rng.randint(10, 20)     # PRs are the primary delivery mechanism
            n_commits   = rng.randint(25, 50)     # 2-4 commits/user/day × 3 users
            n_builds    = rng.randint(10, 18)     # CI triggers on every PR
            n_gates     = 3
            n_vulns     = rng.randint(1, 4)
            n_bugs      = rng.randint(1, 4)
            n_webapp    = rng.randint(1, 3)
            defect_ratio = 0.07

        issues    = _make_issues(rng, fix_version, phase, n_done, n_total, start_dt, release_dt)
        defects   = _make_defects(rng, issues, defect_ratio)
        pipelines = _make_pipelines(rng, fix_version, phase, n_pipelines, release_dt)
        prs       = _make_prs(rng, fix_version, phase, n_prs, start_dt, release_dt)
        commits   = _make_commits(rng, fix_version, phase, n_commits, start_dt, release_dt)
        builds    = _make_builds(rng, phase, n_builds, release_dt)
        gates     = _make_gates(rng, phase, n_gates, release_dt)
        vulns     = _make_vulns(rng, phase, n_vulns, release_dt)
        bugs      = _make_bugs(rng, phase, n_bugs, release_dt)
        webapp    = _make_webapp_vulns(rng, phase, n_webapp)
        tests     = _make_tests(rng, phase)

        row = (
            f"('level_1', '{_LEVEL_VALUE}', '{_LEVEL_VALUE}', "
            f"'{fix_version}', '{_LEVEL_VALUE}', "
            f"'{start_dt}', '{release_dt}', "
            f"'{start_dt}', '{release_dt}', '{status}', "
            f"{_sql_array(issues)}, "
            f"{_sql_array(pipelines)}, "
            f"{_sql_array(prs)}, "
            f"{_sql_array(defects)}, "
            f"{_sql_array(commits)}, "
            f"{_sql_array(builds)}, "
            f"{_sql_array(gates)}, "
            f"{_sql_array(vulns)}, "
            f"{_sql_array(bugs)}, "
            f"{_sql_array(webapp)}, "
            f"{_sql_array(tests)})"
        )
        statements.append(INSERT_SQL.format(catalog=catalog, values=row))

    return statements
