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
"""

import hashlib
import random
from datetime import date, datetime, timedelta

from . import value_stream
from .value_stream import OrgConfig


RECORD_INSERTED_BY = "seed-data-value-stream"


# Distinct failure scenarios. step_name is what the dedup partitions on
# (PARTITION BY pa.step_name, rpd.project_name in pipeline-failures.sql),
# so 10 distinct names × 2 project_names yields ~20 unique table rows max.
_FAILURE_SCENARIOS = [
    ("build-and-test",      "build"),
    ("pytest-unit-tests",   "test"),
    ("eslint",              "quality"),
    ("terraform-apply",     "deploy"),
    ("helm-deploy-staging", "deploy"),
    ("trivy-scan",          "security"),
    ("sonar-quality-gate",  "quality"),
    ("mypy-type-check",     "quality"),
    ("integration-tests",   "test"),
    ("dockerfile-lint",     "quality"),
]


_LOG_TEMPLATES = {
    "build-and-test": (
        "[14:23:01] > tsc --noEmit\n"
        "[14:23:04] src/services/auth.ts:42:14 - error TS2339: Property 'userId' does not exist on type 'AuthContext'.\n"
        "[14:23:04] 42   const id = ctx.userId;\n"
        "[14:23:04]                  ~~~~~~\n"
        "[14:23:04]\n"
        "[14:23:04] src/handlers/login.ts:18:23 - error TS2554: Expected 2 arguments, but got 1.\n"
        "[14:23:04] 18   await authService.login(payload);\n"
        "[14:23:04]                       ~~~~~~~\n"
        "[14:23:04]\n"
        "[14:23:04] Found 2 errors in 2 files.\n"
        "[14:23:04] error Command failed with exit code 2.\n"
        "[14:23:04] ##[error]Process completed with exit code 2."
    ),
    "pytest-unit-tests": (
        "[15:01:22] ============================= test session starts ==============================\n"
        "[15:01:22] platform linux -- Python 3.11.9, pytest-7.4.0, pluggy-1.3.0\n"
        "[15:01:23] collected 234 items\n"
        "[15:01:32]\n"
        "[15:01:32] tests/test_user_service.py::test_create_user_validates_email FAILED          [ 32%]\n"
        "[15:01:32]\n"
        "[15:01:32] =================================== FAILURES ===================================\n"
        "[15:01:32] _____________________ test_create_user_validates_email _____________________\n"
        "[15:01:32]\n"
        "[15:01:32]     def test_create_user_validates_email():\n"
        "[15:01:32]         user = User.create(email='not-an-email')\n"
        "[15:01:32] >       assert user.is_valid()\n"
        "[15:01:32] E       AssertionError: assert False\n"
        "[15:01:32] E        +  where False = <bound method User.is_valid of <User id=42>>\n"
        "[15:01:32]\n"
        "[15:01:32] tests/test_user_service.py:18: AssertionError\n"
        "[15:01:32] =========================== short test summary info ============================\n"
        "[15:01:32] FAILED tests/test_user_service.py::test_create_user_validates_email\n"
        "[15:01:32] ========================= 1 failed, 233 passed in 8.41s ========================\n"
        "[15:01:32] ##[error]Process completed with exit code 1."
    ),
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
    "helm-deploy-staging": (
        "[13:05:11] release 'demo-app-staging' upgrade in progress\n"
        "[13:05:14] waiting for deployment 'demo-app' rollout to finish\n"
        "[13:10:14] error: timed out waiting for the condition\n"
        "[13:10:14]   Deployment 'demo-app' exceeded its progress deadline\n"
        "[13:10:14]   FailedCreate: pods 'demo-app-7b8f9d' forbidden: exceeded quota\n"
        "[13:10:14]\n"
        "[13:10:14] Error: UPGRADE FAILED: timed out waiting for the condition\n"
        "[13:10:14] ##[error]Process completed with exit code 1."
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
        "[10:02:21] CVE-2024-21626: runc filesystem escape (CRITICAL)\n"
        "[10:02:21]   Severity: CRITICAL\n"
        "[10:02:21]   Status: fixed in 1.1.12\n"
        "[10:02:21]\n"
        "[10:02:21] FAIL: 2 critical vulnerabilities found, threshold = 0\n"
        "[10:02:21] ##[error]Process completed with exit code 1."
    ),
    "sonar-quality-gate": (
        "[16:45:23] ANALYSIS SUCCESSFUL, you can find the analysis report at: https://sonar.example.com/dashboard?id=demo-app\n"
        "[16:45:24] Polling Quality Gate status...\n"
        "[16:46:24] Quality Gate status: ERROR\n"
        "[16:46:24]\n"
        "[16:46:24] Failing conditions:\n"
        "[16:46:24]   - Coverage on New Code: 64.3% (required: > 80%)\n"
        "[16:46:24]   - Duplicated Lines on New Code: 8.2% (required: < 3%)\n"
        "[16:46:24]   - Reliability Rating on New Code: C (required: A)\n"
        "[16:46:24]\n"
        "[16:46:24] ##[error]Process completed with exit code 1."
    ),
    "mypy-type-check": (
        "[08:33:02] > mypy --strict src/\n"
        "[08:33:14]\n"
        "[08:33:14] src/handlers/payment.py:78: error: Argument 1 to 'process_payment' has incompatible type 'Optional[str]'; expected 'str'  [arg-type]\n"
        "[08:33:14] src/services/billing.py:124: error: Item 'None' of 'Optional[Customer]' has no attribute 'subscription'  [union-attr]\n"
        "[08:33:14] src/services/billing.py:142: error: Function is missing a type annotation for one or more arguments  [no-untyped-def]\n"
        "[08:33:14]\n"
        "[08:33:14] Found 3 errors in 2 files (checked 87 source files)\n"
        "[08:33:14] ##[error]Process completed with exit code 1."
    ),
    "integration-tests": (
        "[12:18:44] Running integration test suite...\n"
        "[12:18:50] PASS test_login_flow\n"
        "[12:18:55] FAIL test_payment_callback_idempotency\n"
        "[12:18:55]\n"
        "[12:18:55] expected: payment marked as 'paid' on second callback\n"
        "[12:18:55] actual:   payment status remained 'pending'\n"
        "[12:18:55]\n"
        "[12:18:55] traceback:\n"
        "[12:18:55]   File 'tests/integration/test_payment.py', line 87, in test_payment_callback_idempotency\n"
        "[12:18:55]     assert payment.status == 'paid'\n"
        "[12:18:55]   AssertionError\n"
        "[12:18:55]\n"
        "[12:18:55] 1 of 24 tests failed.\n"
        "[12:18:55] ##[error]Process completed with exit code 1."
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
    """One VALUES tuple for base_datasets.pipeline_activities."""
    return (
        "(" + ", ".join([
            _q("github"),                     # pipeline_source
            _q("github"),                     # tool_identifier
            _q("main"),                       # branch
            _q(cfg.project_url),              # project_url
            _q(cfg.project_name),             # project_name
            _q(f["pipeline_id"]),             # pipeline_id
            _q(f["pipeline_name"]),           # pipeline_name
            _q(f["pipeline_url"]),            # pipeline_url
            _q(f["step_id"]),                 # step_id
            _q("failure"),                    # step_conclusion
            _q("completed"),                  # step_status
            _q(f["step_type"]),               # step_type
            _q(f["step_name"]),               # step_name
            _ts(f["started"]),                # step_started_at
            _ts(f["finished"]),               # step_finished_at
            _q("failure"),                    # pipeline_status
            _ts(f["started"]),                # pipeline_started_at
            _ts(f["finished"]),               # pipeline_finished_at
            _q(f["commit_sha"]),              # pipeline_commit_sha
            _q("push"),                       # pipeline_event_type
            _q(cfg.org_name),                 # customer_id
            _q(RECORD_INSERTED_BY),           # record_inserted_by
            _q("github"),                     # data_source
        ]) + ")"
    )


def _rpd_values(cfg: OrgConfig, f: dict) -> str:
    """One VALUES tuple for user_working.repo_pipeline_details."""
    return (
        "(" + ", ".join([
            _q(f["pipeline_id"]),
            _q(cfg.org_name),
            _q(cfg.project_name),
            _q(f["pipeline_name"]),
            _q("failure"),                    # pipeline_status
            _q(f["step_name"]),               # pipeline_step_name
            _q("failure"),                    # pipeline_step_conclusion
            _ts(f["started"]),                # pipeline_started_at
            _ts(f["finished"]),               # pipeline_finished_at
            _q("main"),                       # pipeline_branch
            _q(f["commit_sha"]),              # pipeline_commit_sha
            _q(f["ticket_key"]),              # ticket_key
            _q(RECORD_INSERTED_BY),           # record_inserted_by
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

    # 3-5 distinct failure scenarios per recent ticket, sampled without replacement
    pa_vals, rpd_vals, log_vals = [], [], []

    for ticket_key, ticket_date in _recent_tickets(cfg, story, today):
        n = rng.randint(3, 5)
        scenarios = rng.sample(_FAILURE_SCENARIOS, k=min(n, len(_FAILURE_SCENARIOS)))

        for run_idx, (step_name, step_type) in enumerate(scenarios):
            # Reuse a value_stream pipeline_id for this ticket so flow_row_count > 0
            pipeline_run_idx = (run_idx % 2) + 1
            pipeline_id = f"{cfg.org_name}-{ticket_key}-pipeline-{pipeline_run_idx}"

            started  = datetime.combine(ticket_date, datetime.min.time()) + timedelta(
                hours=10 + run_idx * 2,
                minutes=rng.randint(0, 59),
            )
            finished = started + timedelta(minutes=rng.randint(2, 25))
            commit_sha = hashlib.md5(f"{pipeline_id}-{step_name}".encode()).hexdigest()

            f = {
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
                "log_text":     _LOG_TEMPLATES[step_name],
            }

            pa_vals.append(_pa_values(cfg, f))
            rpd_vals.append(_rpd_values(cfg, f))
            log_vals.append(_log_values(f))

    return {
        "pipeline_activities":  _make_inserts(catalog, "base_datasets.pipeline_activities",          _PA_COLS,   pa_vals),
        "repo_pipeline_details": _make_inserts(catalog, "user_working.repo_pipeline_details",        _RPD_COLS,  rpd_vals),
        "logs":                  _make_inserts(catalog, "user_working.github_offering_workflow_job_logs", _LOGS_COLS, log_vals),
    }
