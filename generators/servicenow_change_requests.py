"""
Generator for transform_stage.trf_servicenow_change_requests.
Simulates ServiceNow change request data for demo-acme-direct.

Story arc:
  - ~2 change requests per business day on average
  - ~40% opsera-originated (correlation_id contains 'opsera')
  - ~60% generic (manual changes, correlation_id is NULL)
  - ~85% resolve as 'Done', ~10% 'Failed', ~5% still 'In Progress'
  - Resolution time: 1-5 days for Done items

Deletion scoped via issue_key LIKE 'demo-seed-chg-%'.
Filter wired via assignment_groups = 'ACME IT Operations'.
"""
import random
from datetime import date, datetime, timedelta
from .utils import date_range, _sql_val

TABLE  = "trf_servicenow_change_requests"
SCHEMA = "transform_stage"

INSERT_SQL = """\
INSERT INTO {catalog}.transform_stage.trf_servicenow_change_requests
  (issue_key, issue_summary, issue_project,
   issue_status, issue_resolution_name,
   issue_start_date, issue_started_at, issue_resolved_at, issue_updated_at,
   correlation_id, itsm_source, scope, cr_class,
   issue_created_by, issue_updated_by, closed_by, assignee_name)
VALUES
{values};"""

ASSIGNMENT_GROUP = "ACME IT Operations"

_SUMMARIES = [
    "Deploy backend service update to production",
    "Database schema migration for user table",
    "Network configuration change for load balancer",
    "SSL certificate renewal for api-gateway",
    "Kubernetes node scaling adjustment",
    "Frontend CDN cache invalidation",
    "Security patch rollout for Linux hosts",
    "API rate limiting policy update",
    "Firewall rule update for internal services",
    "Scheduled maintenance: disk cleanup on app servers",
    "Rolling restart of microservices cluster",
    "Update environment variables for payment service",
    "DNS record update for new subdomain",
    "Redis cache configuration tuning",
    "IAM permission update for CI/CD service account",
]

_STATUSES_DONE   = ["Resolved", "Closed"]
_STATUSES_FAILED = ["Closed"]
_STATUSES_OPEN   = ["In Progress", "Scheduled"]

_SCOPES  = ["Standard", "Standard", "Normal", "Emergency"]
_CLASSES = ["Standard", "Normal", "Emergency", "Standard"]

_USERS = [
    "alice.chen", "bob.martinez", "carol.smith",
    "dave.park", "emma.wilson", "frank.lee",
]


def _opsera_correlation_id(seed: int) -> str:
    rng = random.Random(seed)
    pipeline_id = f"pipe{rng.randint(100, 999)}"
    run = rng.randint(1, 200)
    return f"opsera_{pipeline_id}_deploy_{run}"


def _ts(dt) -> str:
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'" if dt else "NULL"


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    cr_counter = 1
    value_lines = []

    for d in date_range(story["start_date"], story["end_date"]):
        if d.weekday() >= 5:
            continue

        day_rng = random.Random(hash((str(d), "cr")) % (2**31))

        # 0-3 CRs per day, ~30% chance of 0
        n_crs = 0 if day_rng.random() < 0.30 else day_rng.randint(1, 3)

        for seq in range(n_crs):
            rng = random.Random(hash((str(d), seq, "cr_row")) % (2**31))

            issue_key = f"demo-seed-chg-{cr_counter:06d}"
            cr_counter += 1

            summary   = rng.choice(_SUMMARIES)
            creator   = rng.choice(_USERS)
            assignee  = rng.choice(_USERS)
            scope_idx = rng.randint(0, len(_SCOPES) - 1)
            scope     = _SCOPES[scope_idx]
            cr_class  = _CLASSES[scope_idx]

            started_hour = rng.randint(8, 16)
            started_at   = datetime(d.year, d.month, d.day, started_hour, rng.randint(0, 59))

            is_opsera      = rng.random() < 0.40
            correlation_id = _opsera_correlation_id(hash((str(d), cr_counter)) % (2**31)) if is_opsera else None

            resolve_days = rng.randint(1, 5)
            resolved_dt  = started_at + timedelta(days=resolve_days, hours=rng.randint(1, 8))

            outcome = rng.random()
            if resolved_dt.date() > end or outcome >= 0.95:
                resolution_name = None
                status          = rng.choice(_STATUSES_OPEN)
                resolved_dt     = None
                closed_by       = None
            elif outcome < 0.85:
                resolution_name = "Done"
                status          = rng.choice(_STATUSES_DONE)
                closed_by       = rng.choice(_USERS)
            else:
                resolution_name = "Failed"
                status          = rng.choice(_STATUSES_FAILED)
                closed_by       = rng.choice(_USERS)

            updated_dt = resolved_dt if resolved_dt else started_at + timedelta(hours=rng.randint(1, 4))

            value_lines.append(
                f"  ({_sql_val(issue_key)}, {_sql_val(summary)}, {_sql_val(ASSIGNMENT_GROUP)}, "
                f"{_sql_val(status)}, {_sql_val(resolution_name)}, "
                f"DATE '{d.isoformat()}', {_ts(started_at)}, {_ts(resolved_dt)}, {_ts(updated_dt)}, "
                f"{_sql_val(correlation_id)}, 'servicenow', {_sql_val(scope)}, {_sql_val(cr_class)}, "
                f"{_sql_val(creator)}, {_sql_val(assignee)}, {_sql_val(closed_by)}, {_sql_val(assignee)})"
            )

    chunk_size = 500
    statements = []
    for i in range(0, len(value_lines), chunk_size):
        chunk = value_lines[i:i + chunk_size]
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(chunk)))
    return statements
