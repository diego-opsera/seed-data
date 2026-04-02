"""
Generators for SnapLogic dashboard source tables.

Three tables in {catalog}.source_to_stage:
  - raw_snaplogic_snaplex       : one state row per Snaplex per day
  - raw_snaplogic_snaplex_nodes : one snapshot row per node per day
  - raw_snaplogic_activities    : user activity events

All rows scoped to org='demo-acme-direct' for safe, targeted deletion.

Data design
-----------
Three Snaplexes across three environments:
  - demo-snaplex-prod    : 4 nodes, production,  ~95% up
  - demo-snaplex-dev     : 2 nodes, development, ~85% up
  - demo-snaplex-staging : 2 nodes, staging,     ~88% up

Nodes share the same instance_id as their parent Snaplex (snaplex_instance_id).
running_nodes_count / down_nodes_count in the snaplex row are derived from
the actual node statuses generated for that day, keeping the two tables
consistent.
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

# (instance_id, label, environment, location, max_slots, max_mem_mb,
#  node_count, uptime_pct, cpu_cores_per_node, total_mem_gb_per_node)
_SNAPLEXES = [
    ("demo-instance-prod-001", "Demo Production Snaplex",
     "production",  "us-east-1",  40, 8192, 4, 0.95, 16, 64.0),
    ("demo-instance-dev-001",  "Demo Development Snaplex",
     "development", "us-west-2",   8, 4096, 2, 0.85,  8, 32.0),
    ("demo-instance-stg-001",  "Demo Staging Snaplex",
     "staging",     "eu-west-1",  16, 4096, 2, 0.88,  8, 32.0),
]

_USERS    = ["demo-alice@acme.com", "demo-bob@acme.com", "demo-carol@acme.com"]
_PROJECTS = ["demo-etl-pipeline", "demo-analytics-flow", "demo-integration-hub"]
_ASSETS   = [
    "demo-customer-sync", "demo-order-feed", "demo-product-catalog",
    "demo-user-events", "demo-revenue-report", "demo-metric-aggregator",
    "demo-data-quality", "demo-compliance-check",
]
# (event_type, cumulative_weight)
_EVENT_CDF = [("asset_create", 0.30), ("asset_update", 0.85), ("asset_delete", 1.00)]

# Additional users whose first activity is staggered across the story period.
# Each tuple: (email, days_offset_from_story_start)
# Spread ~18 users across ~370 days so every monthly/weekly bucket gets new users.
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
    """Format a datetime as a quoted SQL string Spark auto-casts to TIMESTAMP."""
    return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def _build_daily_state(story: dict):
    """
    Pre-compute per-day, per-Snaplex state so snaplex and node tables are
    consistent (running_nodes_count matches actual node statuses).

    Utilization uses a momentum-based "drifting base" so consecutive days are
    correlated (no more independent white noise). Weekly and monthly rhythms
    are layered on top, plus a hard-coded Q1-end capacity incident on 2026-03-18.

    Incident — 2026-03-18 (Q1 end-of-quarter pipeline crunch):
      Mar 16-17: Pre-incident surge, prod utilization climbs to 90-95%
      Mar 18:    2 of 4 prod nodes fail under load → cc_status='alert',
                 node_availability ~50%, headroom collapses
      Mar 19:    Partial recovery (1 node restored)
      Mar 20+:   Full recovery, utilization normalises

    Returns a dict keyed by (date, instance_id):
      { "cc_status", "running", "down", "reserved", "util", "node_statuses" }
    """
    state = {}
    rng = random.Random(43)

    story_start = date.fromisoformat(story["start_date"])
    story_end   = date.fromisoformat(story["end_date"])
    total_days  = max((story_end - story_start).days, 1)

    INCIDENT_DATE = date(2026, 3, 18)

    # Per-Snaplex drifting utilization base, seeded to the story start
    # Each day the base drifts ±drift_speed, clamped to [floor, ceiling].
    drift_base = {sx[0]: lerp(0.42, 0.55, 0) for sx in _SNAPLEXES}

    for d in date_range(story["start_date"], story["end_date"]):
        t          = (d - story_start).days / total_days
        is_weekend = d.weekday() >= 5
        dow        = d.weekday()          # 0=Mon … 6=Sun
        is_eom     = d.day >= 27          # end-of-month reporting push

        # Incident window relative to INCIDENT_DATE
        incident_delta = (d - INCIDENT_DATE).days   # negative = before, 0 = day-of

        for (inst_id, _label, _env, _loc, max_slots, _mem,
             node_count, uptime_pct, _cpu, _mem_gb) in _SNAPLEXES:

            is_prod = "prod" in inst_id

            # ── Drift the per-Snaplex utilization base ───────────────────────
            # Target: slow upward trend over the year, faster drift on weekdays
            trend_target = lerp(0.45, 0.78, t)
            pull         = (trend_target - drift_base[inst_id]) * 0.08   # gentle pull toward trend
            drift        = rng.uniform(-0.04, 0.04) + pull
            drift_base[inst_id] = max(0.10, min(0.95, drift_base[inst_id] + drift))

            # ── Day-of-week rhythm ───────────────────────────────────────────
            # Mon=1.05, Tue/Wed/Thu=1.10, Fri=0.90, Sat=0.35, Sun=0.25
            dow_mult = [1.05, 1.10, 1.10, 1.10, 0.90, 0.35, 0.25][dow]

            # ── End-of-month burst ───────────────────────────────────────────
            eom_mult = 1.12 if (is_eom and not is_weekend) else 1.0

            # ── Incident override (prod only) ────────────────────────────────
            if is_prod:
                if incident_delta == -2:    # Mar 16: pre-surge begins
                    eom_mult *= 1.25
                elif incident_delta == -1:  # Mar 17: surge peak
                    eom_mult *= 1.40
                elif incident_delta == 0:   # Mar 18: incident day
                    eom_mult *= 1.45
                elif incident_delta == 1:   # Mar 19: partial recovery
                    eom_mult *= 0.85
                # Mar 20+ normalises naturally via drift

            # ── Final utilization ────────────────────────────────────────────
            raw_util = drift_base[inst_id] * dow_mult * eom_mult
            util     = max(0.05, min(0.98, raw_util))

            # ── Incident: force prod Snaplex to alert on Mar 18-19 ───────────
            if is_prod and incident_delta in (0, 1):
                cc_status = "alert"
            else:
                r = rng.random()
                if r < uptime_pct:
                    cc_status = "up_and_running"
                elif r < uptime_pct + 0.06:
                    cc_status = "alert"
                else:
                    cc_status = "not_running"

            # ── Node statuses ────────────────────────────────────────────────
            node_statuses = []
            for ni in range(node_count):
                if is_prod and incident_delta == 0:
                    # Mar 18: nodes 3 and 4 go down
                    node_statuses.append("down" if ni >= 2 else "running")
                elif is_prod and incident_delta == 1:
                    # Mar 19: node 3 restored, node 4 still down
                    node_statuses.append("down" if ni == 3 else "running")
                elif cc_status == "up_and_running":
                    node_statuses.append("running" if rng.random() > 0.02 else "down")
                elif cc_status == "alert":
                    node_statuses.append("running" if rng.random() > 0.40 else "down")
                else:
                    node_statuses.append("down")

            running  = node_statuses.count("running")
            down     = node_statuses.count("down")
            reserved = int(max_slots * util) if cc_status != "not_running" else 0

            state[(d, inst_id)] = {
                "cc_status":     cc_status,
                "running":       running,
                "down":          down,
                "reserved":      reserved,
                "util":          reserved / max_slots if max_slots else 0,
                "node_statuses": node_statuses,
            }

    return state


def generate_snaplex(catalog: str, entities: dict, story: dict) -> list[str]:
    state = _build_daily_state(story)
    rows = []

    for d in date_range(story["start_date"], story["end_date"]):
        for (inst_id, label, env, loc, max_slots, max_mem,
             node_count, _uptime, _cpu, _mem_gb) in _SNAPLEXES:

            s = state[(d, inst_id)]
            t_start = datetime(d.year, d.month, d.day, 0, 0, 0)
            t_end   = t_start + timedelta(hours=23, minutes=59, seconds=59)

            rows.append(
                f"  ({_sql_val(inst_id)}, {_sql_val(label)}, "
                f"{_sql_val(env)}, {_sql_val(loc)}, {_sql_val(_ORG)}, "
                f"{_sql_val(s['cc_status'])}, {s['running']}, {s['down']}, "
                f"{max_slots}, {max_mem}, {s['reserved']}, "
                f"{_ts(t_start)}, {_ts(t_end)})"
            )

    return [
        _INSERT_SNAPLEX.format(catalog=catalog, values=",\n".join(chunk))
        for chunk in _chunks(rows, 200)
    ]


def generate_nodes(catalog: str, entities: dict, story: dict) -> list[str]:
    state = _build_daily_state(story)
    rng   = random.Random(44)
    rows  = []

    for d in date_range(story["start_date"], story["end_date"]):
        for (inst_id, label, env, loc, _max_slots, _max_mem,
             node_count, _uptime, cpu_cores, base_mem_gb) in _SNAPLEXES:

            s = state[(d, inst_id)]

            for n in range(node_count):
                node_id    = f"{inst_id}-node-{n + 1:02d}"
                node_label = f"{label} Node {n + 1}"
                node_status = s["node_statuses"][n]

                # Memory: hardware total is stable (±3%); JVM heap scales
                # with the day's slot utilization so it rises and falls with load.
                util       = s["util"]
                total_mem  = round(base_mem_gb * rng.uniform(0.97, 1.03), 2)
                jvm_pct    = lerp(0.20, 0.45, util) + rng.uniform(-0.03, 0.03)
                jvm_mem    = round(total_mem * max(0.10, min(0.55, jvm_pct)), 2)
                swap_bytes = rng.choice([8_589_934_592, 17_179_869_184])  # 8 GB or 16 GB
                max_fds    = 65536
                create_ts  = datetime(d.year, d.month, d.day,
                                      rng.randint(0, 2), rng.randint(0, 59))

                rows.append(
                    f"  ({_sql_val(node_id)}, {_sql_val(node_label)}, "
                    f"{_sql_val(label)}, {_sql_val(inst_id)}, "
                    f"{_sql_val(env)}, {_sql_val(loc)}, {_sql_val(_ORG)}, "
                    f"{_sql_val(node_status)}, "
                    f"{cpu_cores}, {total_mem}, {jvm_mem}, "
                    f"{swap_bytes}, {max_fds}, "
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

    # Build a set of (user, first_active_date) for onboarding users
    onboarding = {
        user: story_start + timedelta(days=offset)
        for user, offset in _ONBOARDING_USERS
    }

    for d in date_range(story["start_date"], story["end_date"]):
        # Minimal weekend activity
        if d.weekday() >= 5 and rng.random() > 0.10:
            continue

        # Core users: active throughout
        active_users = list(_USERS)
        # Onboarding users: active from their first date onwards
        active_users += [u for u, first in onboarding.items() if d >= first]

        for user in active_users:
            if rng.random() > 0.70:  # ~70% chance active on a given day
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
