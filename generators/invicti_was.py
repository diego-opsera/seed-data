"""
Generator for source_to_stage.raw_invicti_data + raw_invicti_all_issues.

Drives the Web Application Security Overview widget on the Code Reliability
dashboard. SQL (was_overview.sql) joins both tables:
   raw_invicti_data ⋈ filter_groups (on array_contains project_name)
   ⋈ raw_invicti_all_issues (LEFT JOIN on WebsiteId)
The WHERE clause filters on `a.Severity NOT IN ('Information', 'BestPractice')
AND a.IsPresent = true` — so empty raw_invicti_all_issues = empty widget
+ FE crash. We populate BOTH tables; raw_invicti_all_issues is created via
notebooks/code_reliability/create_table_invicti_issues.py first.

Per project: one "Complete" scan within the last 30 days, populated with
realistic VulnerabilityCriticalCount / High / Medium / Low / Info / BestPractice
values matching the org's current phase.

Story (snapshot):
  Acme    : 1-2 critical, 2-3 high, 4-5 medium, 4-6 low per project
  Meridian: 0 critical, 0-1 high, 1-2 medium, 1-3 low per project

Scope tag: record_inserted_by ∈ {'seed-data', 'seed-data-meridian'}.

The table has 104 columns; we only populate the ~20 the widget reads,
plus a handful that "look real" so a sample row passes a sniff test.
The remaining columns default to NULL.
"""
from __future__ import annotations

import hashlib
import random
from datetime import date, datetime, timedelta

from .utils import _sql_val

TABLE = "raw_invicti_data"

ISSUE_TITLES = [
    ("Critical", "SQL Injection in login form"),
    ("Critical", "Remote Code Execution via deserialization"),
    ("High",     "Stored XSS in user profile"),
    ("High",     "Authentication bypass via JWT none algorithm"),
    ("High",     "Missing access control on admin endpoint"),
    ("Medium",   "Reflected XSS in search parameter"),
    ("Medium",   "Insecure cookie missing Secure flag"),
    ("Medium",   "Cleartext submission of password"),
    ("Medium",   "Missing X-Frame-Options header"),
    ("Low",      "Server version disclosure"),
    ("Low",      "Cookie not marked as HttpOnly"),
    ("Low",      "Missing Strict-Transport-Security header"),
]

ISSUES_INSERT_SQL = """\
INSERT INTO {catalog}.source_to_stage.raw_invicti_all_issues
  (WebsiteId, WebsiteName, Severity, State, IsPresent,
   Title, Url, Type, Certainty, LastSeenDate, FirstSeenDate,
   AssigneeName, Description, RemediationDescription, Impact,
   LookupId, record_inserted_by, record_insert_datetime)
VALUES
{values};"""

INSERT_SQL = """\
INSERT INTO {catalog}.source_to_stage.raw_invicti_data
  (Id, WebsiteName, WebsiteUrl, WebsiteId,
   State, Phase, IsCompleted, Percentage, ScanType,
   ThreatLevel, GlobalThreatLevel,
   VulnerabilityCriticalCount, VulnerabilityHighCount,
   VulnerabilityMediumCount, VulnerabilityLowCount,
   VulnerabilityInfoCount, VulnerabilityBestPracticeCount,
   GlobalVulnerabilityCriticalCount, GlobalVulnerabilityHighCount,
   GlobalVulnerabilityMediumCount, GlobalVulnerabilityLowCount,
   GlobalVulnerabilityInfoCount, GlobalVulnerabilityBestPracticeCount,
   TotalVulnerabilityCount,
   InitiatedAt, Initiated, InitiatedDate, InitiatedTime, StateChanged,
   Duration, MaxScanDuration,
   PolicyName, ReportPolicyName,
   tool_id, record_inserted_by, record_insert_datetime, Tags)
VALUES
{values};"""


def _is_meridian(org_name: str) -> bool:
    return org_name == "demo-meridian"


def _vuln_counts(org_name: str, rng: random.Random) -> dict:
    """Severity counts per project per org's current phase."""
    if _is_meridian(org_name):
        return {
            "critical":      rng.randint(0, 1),
            "high":          rng.randint(0, 1),
            "medium":        rng.randint(1, 2),
            "low":           rng.randint(1, 3),
            "info":          rng.randint(0, 2),
            "best_practice": rng.randint(0, 2),
        }
    return {
        "critical":      rng.randint(1, 2),
        "high":          rng.randint(2, 3),
        "medium":        rng.randint(4, 5),
        "low":           rng.randint(4, 6),
        "info":          rng.randint(2, 5),
        "best_practice": rng.randint(1, 4),
    }


def _threat_level(counts: dict) -> str:
    if counts["critical"] > 0:
        return "Critical"
    if counts["high"] > 0:
        return "High"
    if counts["medium"] > 0:
        return "Medium"
    return "Low"


def _website_id(project: str, org_name: str) -> str:
    """Stable GUID-shaped string per project so the dedup partition key is consistent."""
    h = hashlib.sha1(f"{org_name}|{project}".encode()).hexdigest()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _ts_lit(dt: datetime) -> str:
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def _initiated_iso(dt: datetime) -> str:
    """Format like '2025-01-06T01:03:08.660551-08:00' — matches real prod."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f-08:00")


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    record_inserted_by = "seed-data-meridian" if _is_meridian(org_name) else "seed-data"

    end = date.fromisoformat(story["end_date"])

    repos = entities.get("repos", [])
    if not repos:
        return []

    projects = [r["name"].split("/", 1)[-1] for r in repos]

    value_lines = []
    issue_value_lines = []
    for project in projects:
        rng = random.Random(hash((org_name, project, "invicti")) % (2**31))

        # One "Complete" scan within the last 1-30 days
        scan_day = end - timedelta(days=rng.randint(1, 30))
        scan_dt  = datetime(scan_day.year, scan_day.month, scan_day.day,
                            rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59),
                            rng.randint(0, 999999))
        # Scan duration 30s-3min
        scan_secs = rng.randint(30, 180)
        end_dt    = scan_dt + timedelta(seconds=scan_secs)

        counts       = _vuln_counts(org_name, rng)
        threat_level = _threat_level(counts)
        total_vulns  = sum(counts.values())

        scan_id    = _website_id(f"{project}-scan", org_name)
        website_id = _website_id(project, org_name)
        website_url = f"https://{project}.demo.opsera.io/"

        # Duration string like "00:01:17.2688284"
        mins, secs = divmod(scan_secs, 60)
        duration = f"00:{mins:02d}:{secs:02d}.0000000"

        initiated_iso = _initiated_iso(scan_dt)
        state_changed_iso = _initiated_iso(end_dt)

        # InitiatedTime + InitiatedDate use dd/MM/yyyy (matches the SQL's
        # to_timestamp(..., 'dd/MM/yyyy hh:mm a') format). AM/PM uppercase.
        initiated_time_str = scan_dt.strftime('%d/%m/%Y %I:%M %p')
        initiated_date_str = scan_day.strftime('%d/%m/%Y')

        value_lines.append(
            "  ("
            f"{_sql_val(scan_id)}, {_sql_val(project)}, {_sql_val(website_url)}, {_sql_val(website_id)}, "
            f"{_sql_val('Complete')}, {_sql_val('Complete')}, {_sql_val('true')}, {_sql_val('100')}, {_sql_val('Full')}, "
            f"{_sql_val(threat_level)}, {_sql_val(threat_level)}, "
            f"{_sql_val(str(counts['critical']))}, {_sql_val(str(counts['high']))}, "
            f"{_sql_val(str(counts['medium']))}, {_sql_val(str(counts['low']))}, "
            f"{_sql_val(str(counts['info']))}, {_sql_val(str(counts['best_practice']))}, "
            # Global counts mirror local for a single-website scan
            f"{_sql_val(str(counts['critical']))}, {_sql_val(str(counts['high']))}, "
            f"{_sql_val(str(counts['medium']))}, {_sql_val(str(counts['low']))}, "
            f"{_sql_val(str(counts['info']))}, {_sql_val(str(counts['best_practice']))}, "
            f"{_sql_val(str(total_vulns))}, "
            f"{_sql_val(initiated_iso)}, {_sql_val(initiated_iso)}, "
            f"{_sql_val(initiated_date_str)}, "
            f"{_sql_val(initiated_time_str)}, "
            f"{_sql_val(state_changed_iso)}, "
            f"{_sql_val(duration)}, {_sql_val('48')}, "
            f"{_sql_val('Default Security Checks')}, {_sql_val('Default Report Policy')}, "
            f"{_sql_val('seed-data-tool-id')}, "
            f"{_sql_val(record_inserted_by)}, {_ts_lit(end_dt)}, "
            f"{_sql_val('[]')}"
            ")"
        )

        # ── Per-vulnerability rows for raw_invicti_all_issues ────────────────
        # The was_overview.sql LEFT JOIN + WHERE on a.Severity / IsPresent /
        # LastSeenDate filters out rows without IsPresent=true and a matching
        # severity that's NOT in {Information, BestPractice}. We emit one row
        # per counted critical/high/medium/low vuln, plus a couple of info/
        # best-practice rows that the SQL will exclude (so the data looks
        # realistic without breaking the filter).
        # Format MUST match the SQL's to_timestamp(..., 'dd/MM/yyyy hh:mm a').
        # Day first, AM/PM uppercase. NOT lowercased.
        seen_dt_str = scan_dt.strftime("%d/%m/%Y %I:%M %p")
        first_seen  = (scan_dt - timedelta(days=rng.randint(7, 60))).strftime("%d/%m/%Y %I:%M %p")

        per_severity = [
            ("Critical", counts["critical"]),
            ("High",     counts["high"]),
            ("Medium",   counts["medium"]),
            ("Low",      counts["low"]),
            ("Informational", counts["info"]),     # filtered out by SQL — included for realism
            ("BestPractice",  counts["best_practice"]),
        ]
        issue_seq = 0
        for severity, n in per_severity:
            for _ in range(n):
                issue_seq += 1
                title_pool = [t for s, t in ISSUE_TITLES if s == severity]
                title = rng.choice(title_pool) if title_pool else f"{severity} finding"
                url = f"{website_url}vuln-{issue_seq}"
                lookup_id = f"{website_id}-{issue_seq}"
                issue_value_lines.append(
                    "  ("
                    f"{_sql_val(website_id)}, {_sql_val(project)}, {_sql_val(severity)}, "
                    f"{_sql_val('Present')}, TRUE, "
                    f"{_sql_val(title)}, {_sql_val(url)}, {_sql_val('vulnerability')}, "
                    f"{_sql_val('Confirmed')}, {_sql_val(seen_dt_str)}, {_sql_val(first_seen)}, "
                    f"NULL, "
                    f"{_sql_val(f'{title} detected on {project}.')}, "
                    f"{_sql_val('Apply input validation and output encoding.')}, "
                    f"{_sql_val('Could lead to data exposure or compromise.')}, "
                    f"{_sql_val(lookup_id)}, "
                    f"{_sql_val(record_inserted_by)}, {_ts_lit(end_dt)}"
                    ")"
                )

    if not value_lines:
        return []

    statements = [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
    if issue_value_lines:
        statements.append(
            ISSUES_INSERT_SQL.format(catalog=catalog, values=",\n".join(issue_value_lines))
        )
    return statements
