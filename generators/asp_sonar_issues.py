"""
Generator for source_to_stage.raw_sonar_type_data_branchwise.

base_datasets.asp_sonar_issues is a VIEW (`select * from
source_to_stage.raw_sonar_type_data_branchwise`) — we INSERT into the
underlying table; the view will surface the rows automatically.

Drives the Reliability / Security / Maintainability rating widgets on the
Code Reliability dashboard. Each rating category is computed from a count
of issues by `type`:
  - Reliability      ← type = 'BUG'
  - Security         ← type = 'VULNERABILITY'
  - Maintainability  ← type = 'CODE_SMELL'

The dashboard SQL (sonar_ratings_overview.sql) keeps only the 2 most recent
scans per (org_name, project, branch) — it compares "current" vs "previous"
period — so the story arc shows up in the gap between the last 2 scans.

Scan cadence: weekly per project, on Mondays. The exact timestamp string
emitted in `source_record_insert_datetime` is deterministic per project
per Monday, so the asp_sonar_measures generator (next) can reuse the
same timestamps to satisfy the JOIN in sonar_ratings_overview.sql:

    asp_sonar_issues  ⋈  asp_sonar_measures
    ON org_name = org_name
   AND project = project_name
   AND branch = branch
   AND source_record_insert_datetime = source_record_insert_datetime

Story arcs:
  Acme    — steady baseline; security_spikes adds a March 2026 + Nov 2025
            vuln-count surge. CODE_SMELL/BUG counts stay flat.
  Meridian— pre/post-Opsera inflection at t=0.5. Pre-phase has heavy
            CODE_SMELL/BUG load; post-phase shrinks all categories ~70%.

Scope tag for safe delete: record_inserted_by ∈ {'seed-data', 'seed-data-meridian'}.
"""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from .utils import _sql_val, lerp

TABLE = "raw_sonar_type_data_branchwise"

# Insert column list — only the 30 columns we populate. Leaves 7 always-null
# cols (code_variants, clean_code_attribute, clean_code_attribute_category,
# impacts, issue_status, prioritized_rule, cwe) unspecified — Spark fills NULL.
INSERT_SQL = """\
INSERT INTO {catalog}.source_to_stage.raw_sonar_type_data_branchwise
  (org_name, key, rule, severity, component, project, branch,
   line, hash, text_range,
   flows, resolution, status, message, effort, debt, author, tags,
   creation_date, update_date, close_date,
   type, scope, quick_fix_available, message_formattings,
   api_url, data_source,
   source_record_insert_datetime, source_record_update_datetime,
   record_inserted_by)
VALUES
{values};"""


# ── Catalogues ─────────────────────────────────────────────────────────────────

# Real Sonar rule keys grouped by issue type. Sample messages match the rule.
# Distribution skews toward BUG/CODE_SMELL — with extra VULNERABILITY entries
# so the Security rating widget has data (real prod barely has any vulns).
_RULES = {
    "BUG": [
        ("javascript:S2259",  "TypeError: cannot read property of null"),
        ("java:S2259",        "Possible NullPointerException dereference"),
        ("java:S1854",        "Remove this useless assignment"),
        ("python:S5717",      "Mutable default argument leads to shared state"),
        ("typescript:S6606",  "Refactor to avoid using deprecated API"),
        ("javascript:S3403",  "Strict equality comparison always returns false"),
        ("python:S2376",      "Add a missing return statement"),
        ("java:S2589",        "Boolean expression is always true / false"),
    ],
    "VULNERABILITY": [
        ("javascript:S5443",  "Make sure publicly writable directories are used safely"),
        ("python:S4423",      "Disable insecure SSL/TLS protocol versions"),
        ("java:S2068",        "Hard-coded credentials are security-sensitive"),
        ("javascript:S2255",  "Cookies without 'secure' flag are vulnerable"),
        ("java:S5547",        "Cipher algorithms should be strong enough"),
        ("python:S2245",      "Pseudorandom number generators are insecure"),
        ("typescript:S4502",  "Disable CSRF protection only after careful review"),
        ("java:S5145",        "Log forging vulnerability"),
        ("javascript:S3330",  "HTTPS-only flag for cookies must be enabled"),
        ("python:S4426",      "Cryptographic key length must be sufficient"),
    ],
    "CODE_SMELL": [
        ("java:S1068",        "Remove this unused private field"),
        ("java:S106",         "Replace System.out call with proper logging"),
        ("java:S100",         "Method name does not match expected pattern"),
        ("java:S125",         "Remove commented-out code"),
        ("javascript:S3504",  "Unexpected var, use let or const instead"),
        ("javascript:S2814",  "Variable shadowing reduces readability"),
        ("Web:S5254",         "Missing 'lang' attribute on html element"),
        ("python:S1481",      "Remove this unused local variable"),
        ("typescript:S6571",  "Replace this 'any' with a more precise type"),
        ("python:S5754",      "Catch a more specific exception"),
        ("css:S4666",         "Avoid !important rules"),
        ("Web:DoctypePresenceCheck", "Add a <!DOCTYPE> declaration"),
    ],
}

# Severity weights per type (rough match to real prod, with vulnerabilities
# skewed toward higher severity for storytelling visibility).
_SEVERITY_WEIGHTS = {
    "BUG":           [("MAJOR", 65), ("MINOR", 30), ("CRITICAL", 4),  ("BLOCKER", 1)],
    "VULNERABILITY": [("BLOCKER", 30), ("CRITICAL", 35), ("MAJOR", 25), ("MINOR", 10)],
    "CODE_SMELL":    [("MAJOR", 80), ("MINOR", 12), ("CRITICAL", 6),  ("BLOCKER", 1), ("INFO", 1)],
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _weighted_pick(rng: random.Random, weighted_pairs):
    values, weights = zip(*weighted_pairs)
    return rng.choices(values, weights=weights)[0]


def _scan_dates(start: date, end: date) -> list[date]:
    """Weekly Monday timestamps that fall inside the story window."""
    d = start
    while d.weekday() != 0:
        d += timedelta(days=1)
    out = []
    while d <= end:
        out.append(d)
        d += timedelta(days=7)
    return out


def _scan_timestamp_for(project: str, scan_day: date) -> datetime:
    """Deterministic scan timestamp — asp_sonar_measures must compute the
    same value for the JOIN to hit. Keep the formula stable across files."""
    seed = abs(hash(("scan-ts", project, scan_day.isoformat()))) % (24 * 3600)
    hour = (seed // 3600) % 9 + 1   # 01:00–09:59 UTC
    minute = (seed // 60) % 60
    second = seed % 60
    micro = (seed * 977) % 1_000_000
    return datetime(scan_day.year, scan_day.month, scan_day.day, hour, minute, second, micro)


def _ts_lit(dt: datetime | None) -> str:
    if dt is None:
        return "NULL"
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def _array_str(values) -> str:
    if not values:
        return "ARRAY()"
    inner = ", ".join(_sql_val(v) for v in values)
    return f"ARRAY({inner})"


def _text_range_struct(start_line: int, end_line: int, start_offset: int, end_offset: int) -> str:
    return (
        f"NAMED_STRUCT('startLine', {start_line}, 'endLine', {end_line}, "
        f"'startOffset', {start_offset}, 'endOffset', {end_offset})"
    )


# ── Story-arc count helpers ────────────────────────────────────────────────────

def _is_acme(org_name: str) -> bool:
    return org_name == "demo-acme-direct"


def _is_meridian(org_name: str) -> bool:
    return org_name == "demo-meridian"


def _phase_t(scan_day: date, start: date, end: date) -> float:
    total = max((end - start).days, 1)
    return max(0.0, min(1.0, (scan_day - start).days / total))


def _counts_for_scan(org_name: str, scan_day: date, story: dict, start: date, end: date) -> dict:
    """Per-scan issue counts by type. Returns {'BUG': n, 'VULNERABILITY': n, 'CODE_SMELL': n}."""
    if _is_meridian(org_name):
        # Inflection at t=0.5: heavy → light
        t = _phase_t(scan_day, start, end)
        if t < 0.5:
            local_t = t / 0.5
            return {
                "CODE_SMELL":    int(round(lerp(80, 65, local_t))),
                "BUG":           int(round(lerp(25, 18, local_t))),
                "VULNERABILITY": int(round(lerp(8,  6,  local_t))),
            }
        else:
            local_t = (t - 0.5) / 0.5
            return {
                "CODE_SMELL":    int(round(lerp(35, 18, local_t))),
                "BUG":           int(round(lerp(10,  4, local_t))),
                "VULNERABILITY": int(round(lerp(3,   1, local_t))),
            }

    # Acme — steady baseline with optional primary + secondary vuln spike.
    # Windows come from story["events"] (anchored to today).
    base = {"CODE_SMELL": 50, "BUG": 12, "VULNERABILITY": 3}
    if story.get("security_spikes", False):
        events = story.get("events", {})
        if scan_day in events.get("acme_spike_broad", frozenset()):
            base["VULNERABILITY"] = 12
            base["BUG"] = 18
        elif scan_day in events.get("acme_secondary_spike", frozenset()):
            base["VULNERABILITY"] = 7
            base["BUG"] = 14
    return base


# ── Main entrypoint ────────────────────────────────────────────────────────────

def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    record_inserted_by = "seed-data-meridian" if _is_meridian(org_name) else "seed-data"

    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    # Sonar projects ≈ GitHub repo names (without the org prefix). Keeps the
    # filter-wiring step simple later — filter_values_unity.project_name will
    # carry these exact strings.
    repos = entities.get("repos", [])
    if not repos:
        return []

    projects = []
    for r in repos:
        full_name = r["name"]                            # e.g. "demo-acme-direct/backend"
        sonar_project = full_name.split("/", 1)[-1]      # → "backend"
        projects.append(sonar_project)

    user_logins = [u.get("login") for u in entities.get("users", []) if u.get("login")]
    if not user_logins:
        user_logins = [f"{org_name}-bot"]
    authors_pool = [f"{lg}@example.com" for lg in user_logins]

    scan_days = _scan_dates(start, end)

    api_url = "https://sonarqube-demo.opsera.io/api/issues/search"

    issue_counter = 1
    value_lines = []

    for project in projects:
        for scan_day in scan_days:
            scan_dt = _scan_timestamp_for(project, scan_day)
            counts = _counts_for_scan(org_name, scan_day, story, start, end)

            for issue_type, n_issues in counts.items():
                rule_pool = _RULES[issue_type]
                sev_weights = _SEVERITY_WEIGHTS[issue_type]

                for seq in range(n_issues):
                    rng = random.Random(hash((org_name, project, scan_day.isoformat(),
                                              issue_type, seq)) % (2**31))

                    rule, base_message = rng.choice(rule_pool)
                    severity = _weighted_pick(rng, sev_weights)

                    # 95% OPEN, 5% CLOSED-FIXED. CLOSED issues use this scan as their close_date.
                    if rng.random() < 0.05:
                        status, resolution = "CLOSED", "FIXED"
                        close_dt = scan_dt
                    else:
                        status, resolution = "OPEN", None
                        close_dt = None

                    # update_date a few hours before scan_dt; creation_date 1-365 days back
                    update_dt   = scan_dt - timedelta(hours=rng.randint(1, 12),
                                                      minutes=rng.randint(0, 59))
                    creation_dt = update_dt - timedelta(days=rng.randint(1, 365),
                                                        hours=rng.randint(0, 23))

                    # Issue identity — deterministic-ish 20-char-ish key
                    key_seed = abs(hash((org_name, project, issue_counter))) % (10**16)
                    issue_key = f"AZ{key_seed:016X}"
                    issue_counter += 1

                    component = f"{project}:{rng.choice(['src/main.py', 'src/index.js', 'src/server.ts', 'src/Main.java', 'lib/utils.go', 'app/handler.cs'])}"
                    line = rng.randint(1, 800)
                    start_offset = rng.randint(0, 60)
                    end_offset = start_offset + rng.randint(2, 30)
                    text_range = _text_range_struct(line, line, start_offset, end_offset)
                    file_hash = f"{abs(hash((issue_key, 'h'))) & 0xFFFFFFFF:08x}{abs(hash((issue_key, 'h2'))) & 0xFFFFFFFF:08x}"

                    effort = rng.choice(["5min", "10min", "15min", "30min", "1h"])
                    author = rng.choice(authors_pool)
                    tags = rng.choice([
                        ["convention"],
                        ["bad-practice"],
                        ["security"],
                        ["bug"],
                        ["unused"],
                        ["maintainability", "convention"],
                    ])

                    quick_fix = rng.random() < 0.30
                    scope_val = rng.choice(["MAIN", "MAIN", "MAIN", "TEST"])

                    value_lines.append(
                        f"  ({_sql_val(org_name)}, {_sql_val(issue_key)}, {_sql_val(rule)}, "
                        f"{_sql_val(severity)}, {_sql_val(component)}, {_sql_val(project)}, "
                        f"{_sql_val('main')}, "
                        f"{line}, {_sql_val(file_hash)}, {text_range}, "
                        f"{_sql_val('[]')}, {_sql_val(resolution)}, {_sql_val(status)}, "
                        f"{_sql_val(base_message)}, {_sql_val(effort)}, {_sql_val(effort)}, "
                        f"{_sql_val(author)}, {_array_str(tags)}, "
                        f"{_ts_lit(creation_dt)}, {_ts_lit(update_dt)}, {_ts_lit(close_dt)}, "
                        f"{_sql_val(issue_type)}, {_sql_val(scope_val)}, "
                        f"{'TRUE' if quick_fix else 'FALSE'}, ARRAY(), "
                        f"{_sql_val(api_url)}, {_sql_val('sonar_api')}, "
                        f"{_ts_lit(scan_dt)}, {_ts_lit(scan_dt)}, "
                        f"{_sql_val(record_inserted_by)})"
                    )

    if not value_lines:
        return []

    # Chunk to keep individual INSERTs reasonable
    chunk = 400
    statements = []
    for i in range(0, len(value_lines), chunk):
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines[i:i + chunk])))
    return statements


# Exposed for asp_sonar_measures.py to reuse the same scan timestamps so the
# JOIN in sonar_ratings_overview.sql lines up.
def scan_calendar(entities: dict, story: dict) -> list[tuple[str, str, datetime]]:
    """Return list of (project, branch, source_record_insert_datetime) tuples
    that this generator emits — call from asp_sonar_measures.py."""
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])
    repos = entities.get("repos", [])
    projects = [r["name"].split("/", 1)[-1] for r in repos]
    out = []
    for project in projects:
        for scan_day in _scan_dates(start, end):
            out.append((project, "main", _scan_timestamp_for(project, scan_day)))
    return out
