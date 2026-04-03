"""
Generators for SnapLogic dashboard source tables.

Three tables in {catalog}.source_to_stage:
  - raw_snaplogic_snaplex       : state-change event records (NOT a daily time-series)
  - raw_snaplogic_snaplex_nodes : one snapshot per RUNNING node per day
  - raw_snaplogic_activities    : user activity events

All rows scoped to org='demo-acme-direct' for safe, targeted deletion.

Why raw_snaplogic_snaplex is a state-change log, not a daily series
--------------------------------------------------------------------
The trend chart queries (slot-utilization, node-availability, etc.) join
raw_snaplogic_snaplex to raw_snaplogic_snaplex_nodes on instance_id ONLY —
no date correlation. Every bucket therefore gets every Snaplex row via
cross-product and computes the same average → flat line regardless of how
many daily rows we insert.

The queries were designed for a sparse event log: one row per Snaplex per
state change. This gives:
  • Ratio charts (slot util, headroom, node avail): flat at a weighted mean
    of the state periods — acceptable and meaningful.
  • Count charts (status distribution): varies because the cross-product
    count differs per bucket depending on how many node dates fall in it.
  • Node-level trend charts (total memory, CPU, JVM, swap): go directly to
    raw_snaplogic_snaplex_nodes without joining snaplex — fully dynamic.

March 18, 2026 incident (Q1 end-of-quarter pipeline crunch)
------------------------------------------------------------
  Mar 16–17: Pre-incident surge, prod slot utilisation climbs to 97%
  Mar 18:    2 of 4 prod nodes fail under load (cc_status → alert)
  Mar 19:    1 node restored (partial recovery)
  Mar 20+:   Full recovery

Incident is VISIBLE in node-level trend charts as a ~33% drop in total
CPU cores and total memory on Mar 18, partial recovery on Mar 19.
"""

import random
from datetime import date, datetime, timedelta

from .utils import date_range, lerp, _sql_val

TABLE_SNAPLEX    = "raw_snaplogic_snaplex"
TABLE_NODES      = "raw_snaplogic_snaplex_nodes"
TABLE_ACTIVITIES = "raw_snaplogic_activities"

_ORG = "demo-acme-direct"

_INSERT_SNAPLEX = """\
INSERT INTO {catalog}.source_to_stage.raw_snaplogic_snaplex
  (instance_id, label, environment, location, org,
   cc_status, running_nodes_count, down_nodes_count,
   max_slots, max_mem, reserved_slots, time_created, time_updated)
VALUES
{values};"""

_INSERT_NODES = """\
INSERT INTO {catalog}.source_to_stage.raw_snaplogic_snaplex_nodes
  (node_id, node_label, snaplex_name, snaplex_instance_id,
   environment, location, org, node_status,
   cpu_cores, total_memory_gb, jvm_max_mem_gb,
   total_swap_bytes, max_file_descriptors, create_time)
VALUES
{values};"""

_INSERT_ACTIVITIES = """\
INSERT INTO {catalog}.source_to_stage.raw_snaplogic_activities
  (by_whom, org_label, environment, event_type,
   asset_label, project_label, create_time)
VALUES
{values};"""

# Snaplex hardware config: (instance_id, label, env, location, max_slots, max_mem_mb,
#                            node_count, cpu_cores_per_node, base_mem_gb_per_node)
_SNAPLEXES = [
    ("demo-instance-prod-001", "Demo Production Snaplex",
     "production",  "us-east-1",  40, 8192, 4, 16, 64.0),
    ("demo-instance-dev-001",  "Demo Development Snaplex",
     "development", "us-west-2",   8, 4096, 2,  8, 32.0),
    ("demo-instance-stg-001",  "Demo Staging Snaplex",
     "staging",     "eu-west-1",  16, 4096, 2,  8, 32.0),
]

# State-change event log for raw_snaplogic_snaplex.
# Each tuple:
#   (instance_id, cc_status, running_nodes, down_nodes,
#    reserved_frac,  # fraction of max_slots reserved during this period
#    t_start_str, t_end_str)
#
# These ~9 rows are the entire snaplex table content — NOT a daily series.
# The weighted average drives the flat-line Snaplex-level charts:
#   slot_utilization ≈ 69%   capacity_headroom ≈ 31%
#   node_availability ≈ 64%
_SNAPLEX_STATE_PERIODS = [
    # Prod: normal → incident → recovery
    ("demo-instance-prod-001", "up_and_running", 4, 0, 0.80, "2025-03-25", "2026-03-17"),
    ("demo-instance-prod-001", "alert",          2, 2, 0.97, "2026-03-18", "2026-03-19"),
    ("demo-instance-prod-001", "up_and_running", 4, 0, 0.68, "2026-03-20", "2026-04-01"),
    # Dev: normal → brief outage Mar 1-3 → recovery
    ("demo-instance-dev-001",  "up_and_running", 2, 0, 0.52, "2025-03-25", "2026-02-28"),
    ("demo-instance-dev-001",  "not_running",    0, 2, 0.00, "2026-03-01", "2026-03-03"),
    ("demo-instance-dev-001",  "up_and_running", 2, 0, 0.62, "2026-03-04", "2026-04-01"),
    # Staging: normal → brief alert Feb 16-17 → recovery
    ("demo-instance-stg-001",  "up_and_running", 2, 0, 0.44, "2025-03-25", "2026-02-15"),
    ("demo-instance-stg-001",  "alert",          1, 1, 0.50, "2026-02-16", "2026-02-17"),
    ("demo-instance-stg-001",  "up_and_running", 2, 0, 0.62, "2026-02-18", "2026-04-01"),
]

_USERS    = ["demo-alice@acme.com", "demo-bob@acme.com", "demo-carol@acme.com"]
_PROJECTS = ["demo-etl-pipeline", "demo-analytics-flow", "demo-integration-hub"]
_ASSETS   = [
    "demo-customer-sync", "demo-order-feed", "demo-product-catalog",
    "demo-user-events", "demo-revenue-report", "demo-metric-aggregator",
    "demo-data-quality", "demo-compliance-check",
]
_EVENT_CDF = [("asset_create", 0.30), ("asset_update", 0.85), ("asset_delete", 1.00)]

_ONBOARDING_USERS = [
    ("demo-snaplogic-user-01@acme.com",  20),
    ("demo-snaplogic-user-02@acme.com",  45),
    ("demo-snaplogic-user-03@acme.com",  60),
    ("demo-snaplogic-user-04@acme.com",  80),
    ("demo-snaplogic-user-05@acme.com", 100),
    ("demo-snaplogic-user-06@acme.com", 120),
    ("demo-snaplogic-user-07@acme.com", 140),
    ("demo-snaplogic-user-08@acme.com", 160),
    ("demo-snaplogic-user-09@acme.com", 185),
    ("demo-snaplogic-user-10@acme.com", 210),
    ("demo-snaplogic-user-11@acme.com", 230),
    ("demo-snaplogic-user-12@acme.com", 250),
    ("demo-snaplogic-user-13@acme.com", 270),
    ("demo-snaplogic-user-14@acme.com", 290),
    ("demo-snaplogic-user-15@acme.com", 310),
    ("demo-snaplogic-user-16@acme.com", 330),
    ("demo-snaplogic-user-17@acme.com", 350),
    ("demo-snaplogic-user-18@acme.com", 365),
]


def _weighted_event(r):
    for ev, cdf in _EVENT_CDF:
        if r < cdf:
            return ev
    return "asset_delete"


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _ts(dt: datetime) -> str:
    return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def _build_node_daily_state(story: dict) -> dict:
    """
    Return per-day, per-node running/down status for each Snaplex.
    Used only by generate_nodes — not by generate_snaplex.

    Incident hard-coded at 2026-03-18:
      Mar 18: prod nodes 3 and 4 go down
      Mar 19: prod node 3 restored, node 4 still down
      Mar 20+: full recovery
    Other downtime is rare random noise (~2% chance per node per day).

    Returns dict keyed by (date, instance_id) →
      { "node_statuses": ["running"|"down", ...] }
    """
    rng    = random.Random(43)
    result = {}
    INCIDENT = date(2026, 3, 18)

    for d in date_range(story["start_date"], story["end_date"]):
        incident_delta = (d - INCIDENT).days

        for (inst_id, _label, _env, _loc, _slots, _mem,
             node_count, _cpu, _mem_gb) in _SNAPLEXES:

            is_prod = "prod" in inst_id
            statuses = []

            for ni in range(node_count):
                if is_prod and incident_delta == 0:
                    # Mar 18: nodes 3 and 4 down
                    statuses.append("down" if ni >= 2 else "running")
                elif is_prod and incident_delta == 1:
                    # Mar 19: node 4 still down
                    statuses.append("down" if ni == 3 else "running")
                else:
                    # Normal: ~2% random downtime per node
                    statuses.append("down" if rng.random() < 0.02 else "running")

            result[(d, inst_id)] = {"node_statuses": statuses}

    return result


def generate_snaplex(catalog: str, entities: dict, story: dict) -> list[str]:
    """
    Outputs the 9 state-change event records from _SNAPLEX_STATE_PERIODS.
    One INSERT statement for all 9 rows.
    """
    # Build a lookup: instance_id → hardware config
    hw = {sx[0]: sx for sx in _SNAPLEXES}

    rows = []
    for (inst_id, cc_status, running, down, reserved_frac,
         t_start_str, t_end_str) in _SNAPLEX_STATE_PERIODS:

        _, label, env, loc, max_slots, max_mem, _nc, _cpu, _mem = hw[inst_id]
        reserved  = int(max_slots * reserved_frac)
        t_start   = datetime.fromisoformat(t_start_str + " 00:00:00")
        t_end     = datetime.fromisoformat(t_end_str   + " 23:59:59")

        rows.append(
            f"  ({_sql_val(inst_id)}, {_sql_val(label)}, "
            f"{_sql_val(env)}, {_sql_val(loc)}, {_sql_val(_ORG)}, "
            f"{_sql_val(cc_status)}, {running}, {down}, "
            f"{max_slots}, {max_mem}, {reserved}, "
            f"{_ts(t_start)}, {_ts(t_end)})"
        )

    return [_INSERT_SNAPLEX.format(catalog=catalog, values=",\n".join(rows))]


def generate_nodes(catalog: str, entities: dict, story: dict) -> list[str]:
    """
    One snapshot per RUNNING node per day (down nodes don't report).
    Skipping down nodes is what makes CPU/memory trend charts dip during
    incidents — the node-level aggregate charts (total memory, cpu_core_count,
    JVM allocation, swap) go directly to this table without joining snaplex.

    JVM memory grows from ~22% to ~48% of total over the story period to
    simulate a platform under increasing load.
    """
    node_state = _build_node_daily_state(story)
    rng        = random.Random(44)
    rows       = []

    story_start = date.fromisoformat(story["start_date"])
    story_end   = date.fromisoformat(story["end_date"])
    total_days  = max((story_end - story_start).days, 1)

    for d in date_range(story["start_date"], story["end_date"]):
        t = (d - story_start).days / total_days

        for (inst_id, label, env, loc, _slots, _mem,
             node_count, cpu_cores, base_mem_gb) in _SNAPLEXES:

            statuses = node_state[(d, inst_id)]["node_statuses"]

            for n in range(node_count):
                if statuses[n] == "down":
                    continue  # down nodes don't send telemetry

                node_id     = f"{inst_id}-node-{n + 1:02d}"
                node_label  = f"{label} Node {n + 1}"

                # Hardware total is stable (±3%); JVM heap grows over the year
                total_mem = round(base_mem_gb * rng.uniform(0.97, 1.03), 2)
                jvm_pct   = lerp(0.22, 0.48, t) + rng.uniform(-0.03, 0.03)
                jvm_mem   = round(total_mem * max(0.10, min(0.58, jvm_pct)), 2)
                swap_bytes = rng.choice([8_589_934_592, 17_179_869_184])
                create_ts  = datetime(d.year, d.month, d.day,
                                      rng.randint(0, 2), rng.randint(0, 59))

                rows.append(
                    f"  ({_sql_val(node_id)}, {_sql_val(node_label)}, "
                    f"{_sql_val(label)}, {_sql_val(inst_id)}, "
                    f"{_sql_val(env)}, {_sql_val(loc)}, {_sql_val(_ORG)}, "
                    f"'running', "
                    f"{cpu_cores}, {total_mem}, {jvm_mem}, "
                    f"{swap_bytes}, 65536, "
                    f"{_ts(create_ts)})"
                )

    return [
        _INSERT_NODES.format(catalog=catalog, values=",\n".join(chunk))
        for chunk in _chunks(rows, 200)
    ]


def generate_activities(catalog: str, entities: dict, story: dict) -> list[str]:
    rng  = random.Random(45)
    rows = []
    envs = ["production", "development", "staging"]

    story_start = date.fromisoformat(story["start_date"])

    onboarding = {
        user: story_start + timedelta(days=offset)
        for user, offset in _ONBOARDING_USERS
    }

    for d in date_range(story["start_date"], story["end_date"]):
        if d.weekday() >= 5 and rng.random() > 0.10:
            continue

        active_users = list(_USERS)
        active_users += [u for u, first in onboarding.items() if d >= first]

        for user in active_users:
            if rng.random() > 0.70:
                continue

            n_events = rng.randint(1, 5)
            for _ in range(n_events):
                event_type  = _weighted_event(rng.random())
                asset_label = rng.choice(_ASSETS)
                proj_label  = rng.choice(_PROJECTS)
                env         = rng.choice(envs)
                ts = datetime(
                    d.year, d.month, d.day,
                    rng.randint(8, 18), rng.randint(0, 59), rng.randint(0, 59),
                )
                rows.append(
                    f"  ({_sql_val(user)}, {_sql_val(_ORG)}, {_sql_val(env)}, "
                    f"{_sql_val(event_type)}, {_sql_val(asset_label)}, "
                    f"{_sql_val(proj_label)}, {_ts(ts)})"
                )

    return [
        _INSERT_ACTIVITIES.format(catalog=catalog, values=",\n".join(chunk))
        for chunk in _chunks(rows, 500)
    ]
