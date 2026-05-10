"""
Generator for base_datasets.twistlock_security_issues.

Drives the Twistlock Security Overview widget on the Code Reliability dashboard.

Dashboard SQL (twistlock_security_overview.sql) filters:
  tool_identifier = 'twistlock'
  AND tool_data_type = 'container-scan'
Keeps only the LATEST scan per unique_sha_id, JOINs to filter_groups on
project_name, then explodes the cve array and aggregates by severity.

So for the demo we emit a single "current state" scan per project. Per
scan: ~5-8 component rows (java/python/openssl/etc.), each with its own
CVE array. unique_sha_id is stable per scan; component_sha_id varies.

Story (snapshot — overview shows current state only):
  Acme    : 0-1 critical, 1-2 high, 3-5 medium per project
  Meridian: 0 critical, 0-1 high, 2-3 medium per project

Scope tag: record_inserted_by ∈ {'seed-data', 'seed-data-meridian'}.
"""
from __future__ import annotations

import hashlib
import random
from datetime import date, datetime, timedelta

from .utils import _sql_val

TABLE = "twistlock_security_issues"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.twistlock_security_issues
  (activity_date, start_timestamp, end_timestamp,
   tool_identifier, project_name,
   tool_data_pipeline_id, tool_data_scan_duration, tool_data_type,
   tags, data_source,
   source_record_insert_datetime, source_record_update_datetime,
   record_inserted_by,
   image_name, image_version,
   unique_sha_id, component_sha_id, arr_severity,
   component_name, version_number,
   cve, severity, opsera_job_name)
VALUES
{values};"""


# ── CVE catalogue (real GHSA / NVD entries) ────────────────────────────────────
# (component, version, [(cve_id, severity, short_description), ...])
_COMPONENT_CVES = [
    ("java", "17.0.3", [
        ("CVE-2024-20952", "high",     "Java SE Security component vulnerability"),
        ("CVE-2022-34169", "high",     "Apache Xalan integer truncation in XSLTC"),
        ("CVE-2024-21068", "medium",   "Java SE Hotspot vulnerability"),
        ("CVE-2024-21011", "low",      "Java SE Hotspot DoS vulnerability"),
    ]),
    ("python", "3.11.4", [
        ("CVE-2024-0450",  "medium",   "zipfile quoted-overlap in CPython"),
        ("CVE-2023-40217", "medium",   "ssl.SSLSocket bypass on TLS handshake"),
        ("CVE-2023-27043", "low",      "email.parseaddr accepts malformed addresses"),
    ]),
    ("openssl", "3.0.7", [
        ("CVE-2023-5678",  "medium",   "DH_check excessive time with q parameter"),
        ("CVE-2023-2650",  "medium",   "OBJ_obj2txt DoS via crafted X.509 cert"),
        ("CVE-2023-0286",  "high",     "X.400 address type confusion in X.509 GeneralName"),
        ("CVE-2024-0727",  "medium",   "OpenSSL PKCS12 file processing NULL deref"),
    ]),
    ("curl", "8.0.1", [
        ("CVE-2023-46218", "medium",   "Cookie mixed-case PSL bypass"),
        ("CVE-2024-2398",  "medium",   "HTTP/2 push headers memory leak"),
        ("CVE-2024-7264",  "low",      "ASN.1 date parser overread"),
    ]),
    ("nginx", "1.24.0", [
        ("CVE-2024-7347",  "medium",   "ngx_http_mp4_module overflow on crafted MP4"),
        ("CVE-2023-44487", "high",     "HTTP/2 Rapid Reset DDoS"),
    ]),
    ("nodejs", "18.16.0", [
        ("CVE-2024-22019", "high",     "HTTP server DoS via chunk extensions"),
        ("CVE-2023-46809", "medium",   "Marvin attack on RSA-PKCS1-v1_5 decryption"),
        ("CVE-2024-21892", "medium",   "Privilege escalation via env vars"),
    ]),
    ("postgresql-libs", "15.2", [
        ("CVE-2024-0985",  "medium",   "REFRESH MATERIALIZED VIEW CONCURRENTLY priv escalation"),
        ("CVE-2023-5869",  "high",     "Buffer overflow in SQL array modification"),
    ]),
    ("zlib", "1.2.13", [
        ("CVE-2022-37434", "critical", "Heap-based buffer over-read in inflate"),
        ("CVE-2018-25032", "high",     "Memory corruption when compressing certain inputs"),
    ]),
    ("glibc", "2.36", [
        ("CVE-2024-2961",  "high",     "iconv buffer overflow in ISO-2022-CN-EXT"),
        ("CVE-2023-4911",  "high",     "Looney Tunables — local privilege escalation via tunables"),
    ]),
    ("apache-commons-text", "1.10.0", [
        ("CVE-2022-42889", "critical", "Text4Shell — RCE via StringSubstitutor interpolation"),
    ]),
]


def _is_meridian(org_name: str) -> bool:
    return org_name == "demo-meridian"


def _stable_sha(*parts) -> str:
    h = hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()
    return h


def _ts_lit(dt: datetime) -> str:
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def _cve_struct_array(cves: list[tuple[str, str, str]]) -> str:
    """Build SQL ARRAY(NAMED_STRUCT(...), ...) literal matching the cve column type."""
    if not cves:
        return "ARRAY()"
    parts = []
    for cve_id, sev, desc in cves:
        parts.append(
            "NAMED_STRUCT("
            f"'_class', 'twistlock', "
            f"'activityDate', CAST(NULL AS TIMESTAMP_NTZ), "
            f"'author', CAST(NULL AS STRING), "
            f"'cweSeverity', CAST(NULL AS STRING), "
            f"'description', {_sql_val(desc)}, "
            f"'identifier', {_sql_val(cve_id)}, "
            f"'nvdLink', {_sql_val(f'https://nvd.nist.gov/vuln/detail/{cve_id}')}, "
            f"'severity', {_sql_val(sev)}, "
            f"'status', 'NA'"
            ")"
        )
    return f"ARRAY({', '.join(parts)})"


def _tags_array(image_version: str) -> str:
    return f"ARRAY(NAMED_STRUCT('type', 'tag', 'value', {_sql_val(image_version)}))"


def _arr_severity(cves: list[tuple[str, str, str]]) -> str:
    sev_set = sorted({c[1].upper() for c in cves})
    if not sev_set:
        return "ARRAY()"
    return f"ARRAY({', '.join(_sql_val(s) for s in sev_set)})"


def _row_severity(cves: list[tuple[str, str, str]]) -> str:
    """Highest severity for the row-level `severity` column."""
    rank = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}
    if not cves:
        return "LOW"
    top = max(cves, key=lambda c: rank.get(c[1].lower(), 0))
    return top[1].upper()


def _select_cves_for_phase(rng: random.Random, comp_cves: list, org_name: str) -> list:
    """Pick a subset of the component's CVE catalogue weighted by org phase."""
    # Acme post-spike : keep ~75% of medium/high, drop most criticals
    # Meridian       : keep ~50% — even cleaner post-Opsera state
    keep_pct = 0.5 if _is_meridian(org_name) else 0.7
    out = []
    for cve in comp_cves:
        sev = cve[1].lower()
        roll = rng.random()
        if sev == "critical":
            if roll < (0.10 if _is_meridian(org_name) else 0.25):
                out.append(cve)
        elif sev == "high":
            if roll < (0.40 if _is_meridian(org_name) else 0.65):
                out.append(cve)
        elif sev == "medium":
            if roll < keep_pct:
                out.append(cve)
        else:  # low / info
            if roll < 0.50:
                out.append(cve)
    return out


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    record_inserted_by = "seed-data-meridian" if _is_meridian(org_name) else "seed-data"

    end = date.fromisoformat(story["end_date"])

    repos = entities.get("repos", [])
    if not repos:
        return []

    # Same project naming convention as asp_sonar_issues
    projects = [r["name"].split("/", 1)[-1] for r in repos]

    value_lines = []

    for project in projects:
        rng = random.Random(hash((org_name, project, "twistlock")) % (2**31))

        # One scan per project, dated within the last 1-7 days
        scan_day = end - timedelta(days=rng.randint(1, 7))
        scan_dt  = datetime(scan_day.year, scan_day.month, scan_day.day,
                            rng.randint(2, 16), rng.randint(0, 59), rng.randint(0, 59))

        image_name = f"demo-registry.opsera.io/{org_name}/{project}"
        image_version = f"v1.{rng.randint(20, 80)}.{rng.randint(0, 9)}"
        unique_sha_id = _stable_sha(org_name, project, image_version)

        # Pick 5-8 components per scan
        components = rng.sample(_COMPONENT_CVES, k=rng.randint(5, min(8, len(_COMPONENT_CVES))))

        for component_name, version_number, cve_pool in components:
            cves = _select_cves_for_phase(rng, cve_pool, org_name)
            if not cves:
                continue   # Skip components with no surviving CVEs (clean component)

            component_sha_id = _stable_sha(unique_sha_id, component_name, version_number)
            scan_duration = rng.randint(1, 8)

            value_lines.append(
                "  ("
                f"{_ts_lit(scan_dt)}, {_ts_lit(scan_dt - timedelta(seconds=scan_duration))}, {_ts_lit(scan_dt)}, "
                f"{_sql_val('twistlock')}, {_sql_val(project)}, "
                f"NULL, {scan_duration}, {_sql_val('container-scan')}, "
                f"{_tags_array(image_version)}, {_sql_val('twistlock_api')}, "
                f"{_ts_lit(scan_dt)}, {_ts_lit(scan_dt)}, "
                f"{_sql_val(record_inserted_by)}, "
                f"{_sql_val(image_name)}, {_sql_val(image_version)}, "
                f"{_sql_val(unique_sha_id)}, {_sql_val(component_sha_id)}, "
                f"{_arr_severity(cves)}, "
                f"{_sql_val(component_name)}, {_sql_val(version_number)}, "
                f"{_cve_struct_array(cves)}, {_sql_val(_row_severity(cves))}, NULL"
                ")"
            )

    if not value_lines:
        return []

    # Volume here is small (≤ 50 rows total per run), single batch is fine
    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
