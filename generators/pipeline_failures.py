"""
generators/pipeline_failures.py

Generates INSERT statements for the Pipeline Failures feature in
vnxt-insights-api (src/queries/value-stream/pipeline-failures.sql).

The feature joins three tables:
  base_datasets.pipeline_activities (pa)        ←  we INSERT failure rows here
  user_working.repo_pipeline_details (rpd)      ←  we INSERT here
  user_working.github_offering_workflow_job_logs ←  we INSERT here

dora generators populate pipeline_activities with success rows only (no
step_conclusion='failure' rows for demo orgs — confirmed via diag_3.py),
so we add our own failure rows tagged with a distinct
record_inserted_by = 'seed-data-value-stream' for safe deletion.

Pipeline IDs are reused from value_stream.generate's per-ticket pattern
({org_name}-{ticket_key}-pipeline-{N}) so the flow_row_count subquery in
pipeline-failures.sql (which joins back to offerings_jira_pipeline_details)
returns >0 and the page can drill into the linked ticket's flow.

Failures are placed within the last 30 days because pipeline-failures.sql
hardcodes `pipeline_started_at >= DATE_SUB(CURRENT_DATE(), 30)`.

Two phases:
  - Phase A (sporadic): one failure per recent ticket, dated to that ticket
    date, drawn from a 4-step pool (eslint, terraform-apply, trivy-scan,
    dockerfile-lint). Spreads activity across the last 30 days.
  - Phase B (today's narrative incident): a coherent story — vendor library
    auto-patched this morning via Dependabot, breaking API contracts.
    6 themed step failures (build, pytest, integration-tests, helm-deploy,
    sonar, mypy) all dated to `today`, with shared log narrative referencing
    openssl-3.4.0 as the root cause.

Step pools are disjoint so sporadic and incident rows both survive the
dedup-by-(step_name, project_name) in pipeline-failures-count.sql.
"""

import hashlib
import random
from datetime import date, datetime, timedelta

from . import value_stream
from .value_stream import OrgConfig
from .utils import smooth_duration_days


RECORD_INSERTED_BY = "seed-data-value-stream"


# ── Sporadic failure pool — drives Phase A. Disjoint from _INCIDENT_SCENARIOS
# step_names so dedup-by-(step_name, project_name) keeps both phases visible.

_SPORADIC_FAILURE_SCENARIOS = [
    ("eslint",          "quality"),
    ("terraform-apply", "deploy"),
    ("trivy-scan",      "security"),
    ("dockerfile-lint", "quality"),
]


_SPORADIC_LOG_TEMPLATES = {
    "eslint": (
        "[09:14:55] > eslint . --ext .ts,.tsx\n"
        "[09:15:02]\n"
        "[09:15:02] /workspace/src/components/Dashboard.tsx\n"
        "[09:15:02]   34:5  error  'unused' is assigned a value but never used  no-unused-vars\n"
        "[09:15:02]   42:9  error  Missing return type on function              @typescript-eslint/explicit-function-return-type\n"
        "[09:15:02]\n"
        "[09:15:02] /workspace/src/utils/format.ts\n"
        "[09:15:02]   12:1  error  Expected 'no-restricted-imports' violation  no-restricted-imports\n"
        "[09:15:02]\n"
        "[09:15:02] x 3 problems (3 errors, 0 warnings)\n"
        "[09:15:02] ##[error]Process completed with exit code 1."
    ),
    "terraform-apply": (
        "[11:22:08] Terraform v1.6.4\n"
        "[11:22:09] Initializing modules...\n"
        "[11:22:18] Plan: 4 to add, 0 to change, 0 to destroy.\n"
        "[11:22:34] aws_lambda_function.processor: Creating...\n"
        "[11:23:01] Error: error creating Lambda Function: ResourceConflictException:\n"
        "[11:23:01]   Function already exists: processor-staging\n"
        "[11:23:01]   status code: 409, request id: a8b3f...\n"
        "[11:23:01]\n"
        "[11:23:01]   on lambda.tf line 12, in resource 'aws_lambda_function' 'processor':\n"
        "[11:23:01]   12: resource 'aws_lambda_function' 'processor' {\n"
        "[11:23:01]\n"
        "[11:23:01] ##[error]Process completed with exit code 1."
    ),
    "trivy-scan": (
        "[10:02:18] Scanning image: registry.example.com/demo-app:1.2.3\n"
        "[10:02:21]\n"
        "[10:02:21] Total: 18 (UNKNOWN: 0, LOW: 4, MEDIUM: 6, HIGH: 6, CRITICAL: 2)\n"
        "[10:02:21]\n"
        "[10:02:21] CVE-2024-1086: nf_tables use-after-free (CRITICAL)\n"
        "[10:02:21]   Severity: CRITICAL\n"
        "[10:02:21]   Status: fixed in 5.15.149\n"
        "[10:02:21]\n"
        "[10:02:21] FAIL: 2 critical vulnerabilities found, threshold = 0\n"
        "[10:02:21] ##[error]Process completed with exit code 1."
    ),
    "dockerfile-lint": (
        "[07:55:11] > hadolint Dockerfile\n"
        "[07:55:11]\n"
        "[07:55:11] Dockerfile:14 DL3008 warning: Pin versions in apt get install\n"
        "[07:55:11] Dockerfile:14 DL3009 warning: Delete the apt-get lists after installing\n"
        "[07:55:11] Dockerfile:21 DL3025 warning: Use arguments JSON notation for CMD and ENTRYPOINT\n"
        "[07:55:11] Dockerfile:31 DL4006 warning: Set the SHELL option -o pipefail\n"
        "[07:55:11]\n"
        "[07:55:11] 4 issues found\n"
        "[07:55:11] ##[error]Process completed with exit code 1."
    ),
}


# ── Today's narrative incident — Phase B. All 6 step failures share the same
# root cause and reference the same Dependabot PR / CVE so the AI summary +
# any human reading the logs can connect the dots.
#
# Story: an early-morning Dependabot auto-merge bumped openssl 3.3.x → 3.4.0
# to remediate CVE-2026-1147. The new major version removed deprecated APIs
# (createCipher, CipherContext) that several services depend on. Cascading
# CI/test/deploy failures result.

_INCIDENT_THEME = "openssl-3.4.0 CVE auto-patch broke API contract"

_INCIDENT_SCENARIOS = [
    (
        "build-and-test", "build",
        "[09:14:31] > tsc --noEmit\n"
        "[09:14:34] node_modules/openssl/lib/types.d.ts:42:23 - error TS2724: Module 'openssl' has no exported member 'CipherContext'.\n"
        "[09:14:34] Did you mean 'CipherInstance'?\n"
        "[09:14:34]\n"
        "[09:14:34] src/services/auth.ts:18:14 - error TS2305: Module 'openssl' has no exported member 'createCipher'.\n"
        "[09:14:34] 18  import { createCipher } from 'openssl';\n"
        "[09:14:34]\n"
        "[09:14:34] Found 12 errors in 4 files.\n"
        "[09:14:34] ##[error]openssl@3.4.0 removed deprecated APIs (createCipher, CipherContext) — see https://openssl.org/changelog/3.4.0\n"
        "[09:14:34] ##[error]Auto-merged via Dependabot earlier today (PR #4821, CVE-2026-1147)"
    ),
    (
        "pytest-unit-tests", "test",
        "[09:32:11] ============================= test session starts ==============================\n"
        "[09:32:11] platform linux -- Python 3.11.9, pytest-7.4.0\n"
        "[09:32:14] collected 412 items\n"
        "[09:32:42]\n"
        "[09:32:42] tests/security/test_cipher.py::test_aes_encryption FAILED                    [ 12%]\n"
        "[09:32:42] tests/security/test_cipher.py::test_token_signing FAILED                     [ 13%]\n"
        "[09:32:42] tests/auth/test_session.py::test_session_encryption FAILED                   [ 28%]\n"
        "[09:32:42]\n"
        "[09:32:42] _____________ test_aes_encryption _____________\n"
        "[09:32:42]   File 'src/security/cipher.py', line 14, in encrypt\n"
        "[09:32:42]     cipher = crypto.createCipher('aes-256-cbc', key)\n"
        "[09:32:42]              ^^^^^^^^^^^^^^^^^^^\n"
        "[09:32:42] AttributeError: module 'crypto' has no attribute 'createCipher'\n"
        "[09:32:42]\n"
        "[09:32:42] (openssl@3.4.0 removed createCipher; use createCipheriv with explicit IV)\n"
        "[09:32:42]\n"
        "[09:32:42] ========================= 8 failed, 404 passed in 28.13s =========================\n"
        "[09:32:42] ##[error]Process completed with exit code 1."
    ),
    (
        "integration-tests", "test",
        "[10:48:22] Running integration test suite (24 tests)\n"
        "[10:49:01] FAIL test_login_flow                                          [ 12%]\n"
        "[10:49:01]   Auth service returned 503 -- TLS handshake failed\n"
        "[10:49:01]   downstream auth-service crashed at startup:\n"
        "[10:49:01]   Error: openssl: TypeError: Cipher.update is not a function\n"
        "[10:49:01]   at /app/src/auth/jwt.js:42\n"
        "[10:49:01]\n"
        "[10:49:01] FAIL test_signup_flow                                         [ 14%]\n"
        "[10:49:01] FAIL test_password_reset_flow                                 [ 16%]\n"
        "[10:49:01]\n"
        "[10:49:01] (root cause: openssl@3.4.0 auto-merged today via Dependabot,\n"
        "[10:49:01]  removed createCipher API our auth service depends on)\n"
        "[10:49:01]\n"
        "[10:49:01] 8 of 24 tests failed.\n"
        "[10:49:01] ##[error]Process completed with exit code 1."
    ),
    (
        "helm-deploy-staging", "deploy",
        "[11:15:08] Helm deploy aborted: required test gate failed\n"
        "[11:15:08]\n"
        "[11:15:08] Pre-deploy gate check:\n"
        "[11:15:08]   ci-test-pass:        FAIL (12 errors)\n"
        "[11:15:08]   integration-tests:   FAIL (8 of 24)\n"
        "[11:15:08]   sonar-quality-gate:  FAIL (coverage 64.3%)\n"
        "[11:15:08]\n"
        "[11:15:08] Deploy will not proceed until upstream pipelines are green.\n"
        "[11:15:08] Hint: revert openssl bump (Dependabot PR #4821) and re-run.\n"
        "[11:15:08]\n"
        "[11:15:08] ##[error]Process completed with exit code 1."
    ),
    (
        "sonar-quality-gate", "quality",
        "[12:02:18] ANALYSIS SUCCESSFUL -- see https://sonar.example.com/dashboard?id=demo-app\n"
        "[12:02:18] Polling Quality Gate status...\n"
        "[12:03:18] Quality Gate status: ERROR\n"
        "[12:03:18]\n"
        "[12:03:18] Failing conditions:\n"
        "[12:03:18]   - Coverage on New Code: 64.3% (required: > 80%)\n"
        "[12:03:18]     Note: 8 unit tests / 3 integration tests failing -- coverage drops because failed tests do not execute branches\n"
        "[12:03:18]   - New Bugs: 4 (required: 0)\n"
        "[12:03:18]     Source: openssl-3.4.0 API removal (createCipher, CipherContext)\n"
        "[12:03:18]\n"
        "[12:03:18] ##[error]Process completed with exit code 1."
    ),
    (
        "mypy-type-check", "quality",
        "[08:55:42] > mypy --strict src/\n"
        "[08:56:11]\n"
        "[08:56:11] src/security/cipher.py:14: error: Module 'openssl' has no attribute 'createCipher'  [attr-defined]\n"
        "[08:56:11] src/auth/jwt.py:31: error: Argument 1 to 'CipherInstance' has incompatible type 'str'; expected 'bytes'  [arg-type]\n"
        "[08:56:11] src/services/billing.py:47: error: Module 'openssl' has no attribute 'CipherContext'  [attr-defined]\n"
        "[08:56:11]\n"
        "[08:56:11] (openssl@3.4.0 type definitions removed deprecated symbols)\n"
        "[08:56:11]\n"
        "[08:56:11] Found 7 errors in 3 files (checked 87 source files)\n"
        "[08:56:11] ##[error]Process completed with exit code 1."
    ),
]


# ── SQL literal helpers ───────────────────────────────────────────────────────

def _q(s) -> str:
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def _ts(t) -> str:
    if t is None:
        return "NULL"
    return f"TIMESTAMP '{t.isoformat(sep=' ')}'"


# ── Recent-ticket discovery ───────────────────────────────────────────────────

def _recent_tickets(cfg: OrgConfig, story: dict, today: date, days: int = 30):
    """Return [(ticket_key, ticket_date), ...] for tickets within `days` of today."""
    story_start = date.fromisoformat(story["start_date"])
    story_end   = date.fromisoformat(story["end_date"])
    cutoff = today - timedelta(days=days - 1)

    out = []
    for i in range(cfg.ticket_count):
        t = value_stream._ticket_t(cfg, i)
        ticket_date = value_stream._ticket_date(t, story_start, story_end)
        if ticket_date >= cutoff:
            ticket_key = f"{cfg.jira_project}-{cfg.ticket_id_start + i}"
            out.append((ticket_key, ticket_date))
    return out


def _step_id(pipeline_id: str, step_name: str, run_idx: int) -> str:
    """Stable, unique step_id for the (pipeline_id, step_name, run_idx) tuple."""
    return hashlib.md5(f"{pipeline_id}-{step_name}-{run_idx}".encode()).hexdigest()[:16]


# ── Failure record builder ────────────────────────────────────────────────────

def _build_failure(
    cfg: OrgConfig,
    ticket_key: str,
    target_date: date,
    run_idx: int,
    step_name: str,
    step_type: str,
    log_text: str,
    rng: random.Random,
) -> dict:
    """Build the per-failure dict consumed by the three INSERT builders."""
    pipeline_run_idx = (run_idx % 2) + 1
    pipeline_id = f"{cfg.org_name}-{ticket_key}-pipeline-{pipeline_run_idx}"
    started = datetime.combine(target_date, datetime.min.time()) + timedelta(
        hours=8 + (run_idx * 2) % 12,
        minutes=rng.randint(0, 59),
    )
    # Smoothed daily pipeline duration target (~10 min baseline, in days).
    # Incident widens, hotfix narrows — keeps the run-time chart from
    # bouncing between 2 and 25 minutes per failure.
    target_days = smooth_duration_days(
        target_date, 10.0 / (24 * 60), seed_key=(cfg.org_name, "pipeline_dur"),
    )
    duration_minutes = max(1, round(rng.gauss(target_days * 24 * 60, target_days * 24 * 60 * 0.15)))
    finished = started + timedelta(minutes=duration_minutes)
    commit_sha = hashlib.md5(f"{pipeline_id}-{step_name}-{run_idx}".encode()).hexdigest()
    return {
        "ticket_key":   ticket_key,
        "pipeline_id":  pipeline_id,
        "pipeline_name": f"{cfg.project_name}-pipeline",
        "pipeline_url": f"{cfg.project_url}/actions/runs/{rng.randint(100000, 999999)}",
        "step_id":      _step_id(pipeline_id, step_name, run_idx),
        "step_name":    step_name,
        "step_type":    step_type,
        "started":      started,
        "finished":     finished,
        "commit_sha":   commit_sha,
        "log_text":     log_text,
    }


# ── INSERT builders ───────────────────────────────────────────────────────────

_PA_COLS = (
    "pipeline_source, tool_identifier, branch, project_url, project_name, "
    "pipeline_id, pipeline_name, pipeline_url, "
    "step_id, step_conclusion, step_status, step_type, step_name, "
    "step_started_at, step_finished_at, "
    "pipeline_status, pipeline_started_at, pipeline_finished_at, "
    "pipeline_commit_sha, pipeline_event_type, "
    "customer_id, record_inserted_by, data_source"
)

_RPD_COLS = (
    "pipeline_id, org_name, project_name, pipeline_name, pipeline_status, "
    "pipeline_step_name, pipeline_step_conclusion, "
    "pipeline_started_at, pipeline_finished_at, "
    "pipeline_branch, pipeline_commit_sha, ticket_key, record_inserted_by"
)

_LOGS_COLS = "job, logs, record_inserted_by"


def _pa_values(cfg: OrgConfig, f: dict) -> str:
    return (
        "(" + ", ".join([
            _q("github"),                     # pipeline_source
            _q("github"),                     # tool_identifier
            _q("main"),                       # branch
            _q(cfg.project_url),              # project_url
            _q(cfg.project_name),             # project_name
            _q(f["pipeline_id"]),
            _q(f["pipeline_name"]),
            _q(f["pipeline_url"]),
            _q(f["step_id"]),
            _q("failure"),                    # step_conclusion
            _q("completed"),                  # step_status
            _q(f["step_type"]),
            _q(f["step_name"]),
            _ts(f["started"]),                # step_started_at
            _ts(f["finished"]),               # step_finished_at
            _q("failure"),                    # pipeline_status
            _ts(f["started"]),                # pipeline_started_at
            _ts(f["finished"]),               # pipeline_finished_at
            _q(f["commit_sha"]),
            _q("push"),                       # pipeline_event_type
            _q(cfg.org_name),                 # customer_id
            _q(RECORD_INSERTED_BY),
            _q("github"),
        ]) + ")"
    )


def _rpd_values(cfg: OrgConfig, f: dict) -> str:
    return (
        "(" + ", ".join([
            _q(f["pipeline_id"]),
            _q(cfg.org_name),
            _q(cfg.project_name),
            _q(f["pipeline_name"]),
            _q("failure"),                    # pipeline_status
            _q(f["step_name"]),
            _q("failure"),                    # pipeline_step_conclusion
            _ts(f["started"]),
            _ts(f["finished"]),
            _q("main"),
            _q(f["commit_sha"]),
            _q(f["ticket_key"]),
            _q(RECORD_INSERTED_BY),
        ]) + ")"
    )


def _log_values(f: dict) -> str:
    return "(" + ", ".join([
        _q(f["step_id"]),
        _q(f["log_text"]),
        _q(RECORD_INSERTED_BY),
    ]) + ")"


def _make_inserts(catalog: str, table: str, columns: str, values: list[str], batch_size: int = 50) -> list[str]:
    statements = []
    for chunk_start in range(0, len(values), batch_size):
        chunk = values[chunk_start : chunk_start + batch_size]
        statements.append(
            f"INSERT INTO {catalog}.{table} ({columns}) VALUES\n"
            + ",\n".join(chunk)
        )
    return statements


# ── Public entrypoint ─────────────────────────────────────────────────────────

def generate(catalog: str, cfg: OrgConfig, story: dict, today: date) -> dict:
    """
    Build the three sets of INSERT statements for this org.

    Returns a dict {table_label: [INSERT statements]} with three keys:
      - "pipeline_activities"
      - "repo_pipeline_details"
      - "logs"
    """
    rng = random.Random(f"{cfg.org_name}-pipeline-failures")
    recent = _recent_tickets(cfg, story, today)

    pa_vals, rpd_vals, log_vals = [], [], []

    def _record(f):
        pa_vals.append(_pa_values(cfg, f))
        rpd_vals.append(_rpd_values(cfg, f))
        log_vals.append(_log_values(f))

    # ── Phase A: sporadic failures across the last 30 days ────────────────────
    # One failure per recent ticket, dated to that ticket. Round-robin through
    # the 4-step sporadic pool so each project gets all 4 step_names.
    for idx, (ticket_key, ticket_date) in enumerate(recent):
        step_name, step_type = _SPORADIC_FAILURE_SCENARIOS[idx % len(_SPORADIC_FAILURE_SCENARIOS)]
        log_text = _SPORADIC_LOG_TEMPLATES[step_name]
        _record(_build_failure(cfg, ticket_key, ticket_date, idx, step_name, step_type, log_text, rng))

    # ── Phase B: today's narrative incident — openssl-3.4.0 auto-patch ────────
    # All 6 themed step failures dated to `today`. Distributes across recent
    # tickets via round-robin so different incident steps link to different
    # tickets (and the ticket detail view shows the failed step alongside the
    # rest of the issue's flow).
    if recent:
        for inc_idx, (step_name, step_type, log_text) in enumerate(_INCIDENT_SCENARIOS):
            ticket_key, _td = recent[inc_idx % len(recent)]
            _record(_build_failure(cfg, ticket_key, today, inc_idx, step_name, step_type, log_text, rng))

    return {
        "pipeline_activities":   _make_inserts(catalog, "base_datasets.pipeline_activities",          _PA_COLS,   pa_vals),
        "repo_pipeline_details": _make_inserts(catalog, "user_working.repo_pipeline_details",         _RPD_COLS,  rpd_vals),
        "logs":                  _make_inserts(catalog, "user_working.github_offering_workflow_job_logs", _LOGS_COLS, log_vals),
    }
