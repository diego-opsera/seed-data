"""
Generator for base_datasets.dependabot_scan_alert.

Produces GitHub Advanced Security (GHAS) Dependency Scanning alerts — the
third series in the Code Reliability dashboard's "GitHub Security" widget
(alongside code_scan_alert and secret_scan_alert which are already seeded).

Mirrors code_scan_alert.py:
  - Baseline ~4 alerts/weekday from a small CVE catalogue
  - Spike days for the Acme SEV1 story (March 2026 + Nov 2025) when
    story["security_spikes"] is True
  - State machine: ~70% fixed, ~5% dismissed, ~25% open
  - Severity distribution roughly matches real prod data
    (medium 46%, high 35%, low 11%, critical 8%)

All date columns are STRING in the table (e.g. "2025-10-07T06:50:31Z"), not
TIMESTAMP — the INSERT emits string literals, not TIMESTAMP literals.
"""
import json
import random
from datetime import date, datetime, timedelta

from .utils import date_range, _sql_val

TABLE = "dependabot_scan_alert"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.dependabot_scan_alert
  (number, org_name, organization, state, severity,
   created_at, updated_at, fixed_at, dismissed_at,
   dismissed_by, dismissed_reason,
   ecosystem, package_name, manifest_path, location_path, scope,
   ghsa_id, cve_id, summary, description, identifiers,
   published_at, security_advisory_updated_at,
   vulnerabilities, cvss_severities, cvss, cwes, security_vulnerability,
   repository_id, repository_node_id, repository_name,
   url, html_url, repository_html_url,
   teams, authors)
VALUES
{values};"""

# Severity distribution (weights mirror real prod ratios)
_SEVERITIES = (
    ["medium"] * 46
    + ["high"]   * 35
    + ["low"]    * 11
    + ["critical"] * 8
)

# Tiny CVE catalogue keyed by ecosystem. Real GHSA/CVE IDs taken from public
# advisories so identifiers, cvss strings, and cwes look authentic.
_CVE_CATALOGUE = [
    # (ecosystem, package, ghsa_id, cve_id, default_severity, summary, vulnerable_range, fix_version, cwe_id, cwe_name)
    ("npm",      "axios",                "GHSA-jr5f-v2jv-69x6", "CVE-2024-39338", "high",     "Axios SSRF via absolute URL",                      "< 1.7.4",   "1.7.4",   "CWE-918", "Server-Side Request Forgery (SSRF)"),
    ("npm",      "lodash",               "GHSA-35jh-r3h4-6jhm", "CVE-2021-23337", "high",     "Lodash command injection via template",            "< 4.17.21", "4.17.21", "CWE-77",  "Improper Neutralization of Special Elements used in a Command"),
    ("npm",      "webpack-dev-server",   "GHSA-9jgg-88mc-972h", "CVE-2024-29180", "medium",   "webpack-dev-server source code theft",             "< 5.2.0",   "5.2.0",   "CWE-201", "Insertion of Sensitive Information Into Sent Data"),
    ("npm",      "@eslint/plugin-kit",   "GHSA-7q7g-4xm8-89cq", "CVE-2024-21539", "low",      "ReDoS in @eslint/plugin-kit",                      "< 0.2.3",   "0.2.3",   "CWE-1333","Inefficient Regular Expression Complexity"),
    ("npm",      "path-to-regexp",       "GHSA-9wv6-86v2-598j", "CVE-2024-45296", "high",     "path-to-regexp ReDoS via crafted patterns",        "< 0.1.10",  "0.1.10",  "CWE-1333","Inefficient Regular Expression Complexity"),
    ("pip",      "urllib3",              "GHSA-34jh-p97f-mpxf", "CVE-2024-37891", "medium",   "urllib3 strips Proxy-Authorization on redirects",  "< 2.2.2",   "2.2.2",   "CWE-200", "Exposure of Sensitive Information"),
    ("pip",      "requests",             "GHSA-9wx4-h78v-vm56", "CVE-2024-35195", "medium",   "Requests session verify bypass after redirect",    "< 2.32.0",  "2.32.0",  "CWE-670", "Always-Incorrect Control Flow Implementation"),
    ("pip",      "cryptography",         "GHSA-h4gh-qq45-vh27", "CVE-2024-26130", "high",     "cryptography NULL-pointer deref in PKCS12 parsing","< 42.0.4",  "42.0.4",  "CWE-476", "NULL Pointer Dereference"),
    ("pip",      "jinja2",               "GHSA-h75v-3vvj-5mfj", "CVE-2024-34064", "medium",   "Jinja2 xmlattr filter accepts non-attr keys",      "< 3.1.4",   "3.1.4",   "CWE-79",  "Cross-site Scripting"),
    ("pip",      "pypdf",                "GHSA-7hfw-26vp-jp8m", "CVE-2025-55197", "medium",   "PyPDF FlateDecode RAM exhaustion",                 "< 6.0.0",   "6.0.0",   "CWE-400", "Uncontrolled Resource Consumption"),
    ("pip",      "tensorflow",           "GHSA-3rcw-9p9x-582v", "CVE-2024-31571", "high",     "TensorFlow heap overflow in tf.raw_ops.Dilation2D","< 2.16.2",  "2.16.2",  "CWE-787", "Out-of-bounds Write"),
    ("go",       "golang.org/x/net",     "GHSA-4v7x-pqxf-cx7m", "CVE-2024-45338", "high",     "golang.org/x/net htmlquery quadratic complexity",  "< 0.33.0",  "0.33.0",  "CWE-1333","Inefficient Regular Expression Complexity"),
    ("go",       "golang.org/x/crypto",  "GHSA-v778-237x-gjrc", "CVE-2024-45337", "critical", "golang.org/x/crypto SSH server auth bypass",       "< 0.31.0",  "0.31.0",  "CWE-287", "Improper Authentication"),
    ("maven",    "com.fasterxml.jackson.core:jackson-databind", "GHSA-jjjh-jjxp-wpff", "CVE-2023-35116", "medium", "jackson-databind cyclic deps DoS", "< 2.15.2", "2.15.2", "CWE-674", "Uncontrolled Recursion"),
    ("maven",    "org.springframework:spring-web",              "GHSA-4wrc-f8pq-fpqp", "CVE-2024-22243", "high",   "Spring URL parsing open redirect","< 6.1.4",  "6.1.4",  "CWE-601", "URL Redirection to Untrusted Site"),
    ("rubygems", "nokogiri",             "GHSA-xc9x-jj77-9p9j", "CVE-2024-34459", "medium",   "Nokogiri buffer overread in libxml2",              "< 1.16.5",  "1.16.5",  "CWE-126", "Buffer Over-read"),
    ("composer", "symfony/http-kernel",  "GHSA-q3r4-x4qw-hqjr", "CVE-2024-50342", "medium",   "Symfony http-kernel cookie parsing inconsistency", "< 6.4.18",  "6.4.18",  "CWE-444", "HTTP Request/Response Smuggling"),
]

# Spike volumes sourced from story["events"]["acme_dependabot_spike_volumes"]
# — anchored relative to today via generators.utils.ACME_DEPENDABOT_SPIKE_VOLUMES.


def _iso_z(dt: datetime) -> str:
    """ISO-8601 string with Z suffix — matches the format real rows use."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _array_str(values) -> str:
    """SQL ARRAY literal of strings, or NULL when input is empty/None."""
    if not values:
        return "NULL"
    inner = ", ".join(_sql_val(v) for v in values)
    return f"ARRAY({inner})"


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    repos = [(r["name"], r["html_url"]) for r in entities.get("repos", [])]
    if not repos:
        return []

    team_names = [t["name"] for t in entities.get("teams", [])]
    teams_pool = [[t] for t in team_names]
    if len(team_names) >= 2:
        teams_pool.append(team_names)

    user_logins = [u.get("login") for u in entities.get("users", []) if u.get("login")]
    if not user_logins:
        user_logins = [f"{org_name}-bot"]

    is_meridian = (org_name == "demo-meridian")
    spike_days = (story.get("events", {}).get("acme_dependabot_spike_volumes", {})
                  if story.get("security_spikes", False) else {})

    # Meridian pre/post-Opsera inflection at t=0.5 — pre-phase has heavier
    # alert volume (manual dep mgmt, slower fixes), post-phase ramps down
    # (Opsera-driven Renovate-style auto-PRs catching alerts faster).
    total_days = max((end - start).days, 1)
    inflection_day = start + timedelta(days=total_days // 2)

    def meridian_alerts_for(d: date) -> int:
        """Pre-Opsera: ~1.5 alerts/weekday; post-Opsera: ramps from 1.0 → 0.3."""
        day_rng = random.Random(hash((str(d), "dep_count_meridian")) % (2**31))
        if d < inflection_day:
            return day_rng.choices([0, 1, 2, 3], weights=[15, 30, 35, 20])[0]
        # Post-inflection: linear decay over the back half
        post_t = (d - inflection_day).days / max((end - inflection_day).days, 1)
        return day_rng.choices([0, 1, 2], weights=[
            40 + int(40 * post_t),                # 0-alerts share grows over time
            45 - int(20 * post_t),
            15 - int(20 * post_t),
        ])[0]

    # Per-repo synthetic IDs (kept stable per repo across runs)
    def repo_ids(repo_name):
        h = abs(hash(("dep-rid", repo_name))) % (10**9)
        return str(1000_000_000 + h), f"R_kgDO{h:010d}"

    alert_counter = 1
    value_lines = []

    for d in date_range(story["start_date"], story["end_date"]):
        if d in spike_days:
            n_alerts = spike_days[d]
        elif d.weekday() >= 5:
            continue
        elif is_meridian:
            n_alerts = meridian_alerts_for(d)
        else:
            day_rng = random.Random(hash((str(d), "dep_count", org_name)) % (2**31))
            # ~0.8 alerts per weekday = ~4/week (same cadence as code_scan)
            n_alerts = day_rng.choices([0, 1, 2], weights=[40, 45, 15])[0]

        for seq in range(n_alerts):
            rng = random.Random(hash((str(d), seq, "dep", org_name)) % (2**31))

            repo_name, repo_url = rng.choice(repos)
            repo_id, repo_node_id = repo_ids(repo_name)
            adv = rng.choice(_CVE_CATALOGUE)
            (ecosystem, package, ghsa_id, cve_id, default_sev, summary,
             vuln_range, fix_version, cwe_id, cwe_name) = adv

            # Mostly use the advisory's natural severity but jitter occasionally
            severity = default_sev if rng.random() < 0.7 else rng.choice(_SEVERITIES)

            number = alert_counter
            alert_counter += 1

            # Created timestamp — daytime hour, weekday only outside spikes
            created_dt = datetime(d.year, d.month, d.day,
                                  rng.randint(8, 18), rng.randint(0, 59), rng.randint(0, 59))
            created_at = _iso_z(created_dt)

            # State machine
            roll = rng.random()
            state = "fixed" if roll < 0.70 else ("dismissed" if roll < 0.75 else "open")

            fixed_at = None
            dismissed_at = None
            dismissed_by = None
            dismissed_reason = None
            updated_dt = created_dt + timedelta(hours=rng.randint(1, 12))

            if state == "fixed":
                # Meridian post-inflection: alerts auto-PR'd within 1-3 days.
                # Pre-inflection or Acme: 70% in 1-14 days, 30% lingering 15-60.
                if is_meridian and d >= inflection_day:
                    fix_dt = created_dt + timedelta(days=rng.randint(1, 3),
                                                    hours=rng.randint(0, 23))
                elif rng.random() < 0.70:
                    fix_dt = created_dt + timedelta(days=rng.randint(1, 14),
                                                    hours=rng.randint(0, 23))
                else:
                    fix_dt = created_dt + timedelta(days=rng.randint(15, 60),
                                                    hours=rng.randint(0, 23))
                if fix_dt.date() <= end:
                    fixed_at = _iso_z(fix_dt)
                    updated_dt = fix_dt
                else:
                    state = "open"
                    fixed_at = None
            elif state == "dismissed":
                dismiss_dt = created_dt + timedelta(days=rng.randint(7, 45),
                                                    hours=rng.randint(0, 23))
                if dismiss_dt.date() <= end:
                    dismissed_at = _iso_z(dismiss_dt)
                    dismissed_by = rng.choice(user_logins)
                    dismissed_reason = rng.choice(["not_used", "tolerable_risk", "no_bandwidth", "fix_started"])
                    updated_dt = dismiss_dt
                else:
                    state = "open"

            updated_at = _iso_z(updated_dt)

            # Published / advisory updated dates (precede created_at by 1-180 days)
            published_dt = created_dt - timedelta(days=rng.randint(1, 180),
                                                  hours=rng.randint(0, 23))
            advisory_updated_dt = published_dt + timedelta(days=rng.randint(0, 14))
            published_at = _iso_z(published_dt)
            adv_updated_at = _iso_z(advisory_updated_dt)

            # Manifest path varies per ecosystem
            manifest_path = {
                "npm":      "package-lock.json",
                "pip":      rng.choice(["requirements.txt", "poetry.lock", "Pipfile.lock"]),
                "go":       "go.sum",
                "maven":    "pom.xml",
                "rubygems": "Gemfile.lock",
                "composer": "composer.lock",
            }.get(ecosystem, "package-lock.json")

            scope = "development" if rng.random() < 0.20 else "runtime"

            html_url = f"https://github.com/{repo_name}/security/dependabot/{number}"
            api_url  = f"https://api.github.com/repos/{repo_name}/dependabot/alerts/{number}"

            # JSON blob columns the dashboard SQL doesn't read but real rows have
            identifiers_json = json.dumps([
                {"value": ghsa_id, "type": "GHSA"},
                {"value": cve_id,  "type": "CVE"},
            ])
            vuln_obj = {
                "package": {"ecosystem": ecosystem, "name": package},
                "severity": severity,
                "vulnerable_version_range": vuln_range,
                "first_patched_version": {"identifier": fix_version},
            }
            vulnerabilities_json = json.dumps([vuln_obj])
            security_vulnerability_json = json.dumps(vuln_obj)
            cvss_score = round({"low": 3.5, "medium": 5.5, "high": 7.5, "critical": 9.5}[severity]
                               + rng.uniform(-0.4, 0.4), 1)
            cvss_obj = {"vector_string": None, "score": cvss_score}
            cvss_json = json.dumps(cvss_obj)
            cvss_severities_json = json.dumps({
                "cvss_v3": cvss_obj,
                "cvss_v4": {"vector_string": None, "score": 0.0},
            })
            cwes_json = json.dumps([{"cwe_id": cwe_id, "name": cwe_name}])

            description = f"{summary}. Affects {package} versions {vuln_range}; upgrade to {fix_version}."

            # Teams + authors arrays (90% populated in real data)
            teams = rng.choice(teams_pool) if teams_pool and rng.random() < 0.90 else None
            n_authors = rng.randint(1, 3)
            authors = rng.sample(user_logins, k=min(n_authors, len(user_logins))) if rng.random() < 0.90 else None

            value_lines.append(
                f"  ({_sql_val(str(number))}, {_sql_val(org_name)}, {_sql_val(org_name)}, "
                f"{_sql_val(state)}, {_sql_val(severity)}, "
                f"{_sql_val(created_at)}, {_sql_val(updated_at)}, "
                f"{_sql_val(fixed_at)}, {_sql_val(dismissed_at)}, "
                f"{_sql_val(dismissed_by)}, {_sql_val(dismissed_reason)}, "
                f"{_sql_val(ecosystem)}, {_sql_val(package)}, "
                f"{_sql_val(manifest_path)}, {_sql_val(manifest_path)}, "
                f"{_sql_val(scope)}, "
                f"{_sql_val(ghsa_id)}, {_sql_val(cve_id)}, "
                f"{_sql_val(summary)}, {_sql_val(description)}, "
                f"{_sql_val(identifiers_json)}, "
                f"{_sql_val(published_at)}, {_sql_val(adv_updated_at)}, "
                f"{_sql_val(vulnerabilities_json)}, {_sql_val(cvss_severities_json)}, "
                f"{_sql_val(cvss_json)}, {_sql_val(cwes_json)}, "
                f"{_sql_val(security_vulnerability_json)}, "
                f"{_sql_val(repo_id)}, {_sql_val(repo_node_id)}, {_sql_val(repo_name)}, "
                f"{_sql_val(api_url)}, {_sql_val(html_url)}, {_sql_val(repo_url)}, "
                f"{_array_str(teams)}, {_array_str(authors)})"
            )

    if not value_lines:
        return []

    # Chunk to keep INSERT statements manageable
    chunk = 400
    statements = []
    for i in range(0, len(value_lines), chunk):
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines[i:i + chunk])))
    return statements
