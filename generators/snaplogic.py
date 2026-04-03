"""
Generators for SnapLogic dashboard source tables.

Three tables in {catalog}.source_to_stage:
  - raw_snaplogic_snaplex       : ONE current-state row per Snaplex (10 total)
  - raw_snaplogic_snaplex_nodes : daily snapshots, ONLY for online instances
  - raw_snaplogic_activities    : user activity events

All rows scoped to org='demo-acme-direct'.

Why variation requires complete instance outages
-------------------------------------------------
The trend queries join raw_snaplogic_snaplex to raw_snaplogic_snaplex_nodes
on instance_id only (no date correlation). Every bucket therefore gets every
Snaplex row in a cross-product → same sum every day → flat line.

The ONLY escape: an instance with no node records on a given date is absent
from the source CTE and excluded from that bucket's aggregate. If the absent
instances have different utilization rates than the rest, the ratio changes.

Design
------
10 Snaplexes across 3 environments. Two high-load prod instances (prod-003,
prod-004 at 95%/93% reserved) crash completely on March 18–19, 2026,
removing their outsized reserved_slots contribution from those buckets:

  Normal (all 10 online):   slot_util ≈ 77%  headroom ≈ 23%
  Mar 18–19 incident:       slot_util ≈ 68%  headroom ≈ 32%

That ~9pp swing is clearly visible in daily-bucket charts.

Additional variation:
  - stg-004 comes online March 15 → small step change in capacity metrics
  - dev-001 goes offline Mar 1–3 (outside the default 30-day view)
  - JVM memory grows from 22% → 48% of node total over the story year
  - CPU/memory charts dip whenever any instance is offline (no node records)

March 18, 2026 incident narrative (Q1 end-of-quarter pipeline crunch)
----------------------------------------------------------------------
  Mar 16–17 : pre-incident surge, all 10 instances online
  Mar 18     : prod-003 and prod-004 crash (ALL nodes offline)
               slot_util drops 77% → 68%  |  headroom jumps 23% → 32%
  Mar 19     : same — systems still offline
  Mar 20     : full recovery, all 10 online
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

# Current-state snapshot for each Snaplex.
# One row per instance — this is the entire raw_snaplogic_snaplex table content.
#
# Columns:
#   instance_id, label, env, location,
#   max_slots, max_mem_mb,
#   running_nodes, down_nodes, cc_status, reserved_slots,
#   story_start_str (time_created), story_end_str (time_updated),
#   node_count, cpu_cores_per_node, base_mem_gb_per_node,
#   online_from_str (None = from story start), offline_dates (set of date strings)
#
# prod-003 and prod-004 are the high-load instances that crash on Mar 18–19.
# Their high reserved_slots (38, 30) are excluded from those two buckets,
# causing the visible dip in slot_utilization and spike in capacity_headroom.

_STORY_START = "2025-03-25"
_STORY_END   = "2026-04-01"

_SNAPLEXES = [
    # id,                    label,                             env,           loc,           max, mem,   run, dn, status,          res,  t_start,      t_end,        n, cpu, mem_gb, online_from,   offline_dates
    ("demo-instance-prod-001", "Demo Production Snaplex A",     "production",  "us-east-1",   40, 8192,  4,   0, "up_and_running", 36,   _STORY_START, _STORY_END,  4,  16,  64.0,  None,          set()),
    ("demo-instance-prod-002", "Demo Production Snaplex B",     "production",  "us-east-1",   40, 8192,  4,   0, "up_and_running", 34,   _STORY_START, _STORY_END,  4,  16,  64.0,  None,          set()),
    ("demo-instance-prod-003", "Demo Production Snaplex C",     "production",  "us-east-2",   40, 8192,  4,   0, "up_and_running", 38,   _STORY_START, _STORY_END,  4,  16,  64.0,  None,          {"2026-03-18", "2026-03-19"}),
    ("demo-instance-prod-004", "Demo Production Snaplex D",     "production",  "us-east-2",   32, 8192,  4,   0, "up_and_running", 30,   _STORY_START, _STORY_END,  4,  16,  64.0,  None,          {"2026-03-18", "2026-03-19"}),
    ("demo-instance-dev-001",  "Demo Development Snaplex A",    "development", "us-west-2",    8, 4096,  2,   0, "up_and_running",  4,   _STORY_START, _STORY_END,  2,   8,  32.0,  None,          {"2026-03-01", "2026-03-02", "2026-03-03"}),
    ("demo-instance-dev-002",  "Demo Development Snaplex B",    "development", "us-west-2",    8, 4096,  2,   0, "up_and_running",  3,   _STORY_START, _STORY_END,  2,   8,  32.0,  None,          set()),
    ("demo-instance-stg-001",  "Demo Staging Snaplex A",        "staging",     "eu-west-1",   16, 4096,  2,   0, "up_and_running",  8,   _STORY_START, _STORY_END,  2,   8,  32.0,  None,          set()),
    ("demo-instance-stg-002",  "Demo Staging Snaplex B",        "staging",     "eu-west-1",   16, 4096,  2,   0, "up_and_running",  5,   _STORY_START, _STORY_END,  2,   8,  32.0,  None,          set()),
    ("demo-instance-stg-003",  "Demo Staging Snaplex C",        "staging",     "ap-southeast-1", 16, 4096, 2, 0, "up_and_running", 9,  "2026-02-01", _STORY_END,  2,   8,  32.0,  "2026-02-01",  set()),
    ("demo-instance-stg-004",  "Demo Staging Snaplex D",        "staging",     "ap-southeast-1",  8, 4096, 2, 0, "up_and_running", 5,  "2026-03-15", _STORY_END,  2,   8,  32.0,  "2026-03-15",  set()),
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


def _is_online(sx, d: date) -> bool:
    """Return True if this Snaplex instance should have node records on date d."""
    *_, online_from_str, offline_dates = sx
    if online_from_str and d < date.fromisoformat(online_from_str):
        return False
    if d.isoformat() in offline_dates:
        return False
    return True


def generate_snaplex(catalog: str, entities: dict, story: dict) -> list[str]:
    """One current-state row per Snaplex (10 rows total)."""
    rows = []
    for sx in _SNAPLEXES:
        (inst_id, label, env, loc, max_slots, max_mem,
         running, down, cc_status, reserved,
         t_start_str, t_end_str,
         _n, _cpu, _mem, _online_from, _offline) = sx

        t_start = datetime.fromisoformat(t_start_str + " 00:00:00")
        t_end   = datetime.fromisoformat(t_end_str   + " 23:59:59")

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
    Daily node snapshots, skipping days when the instance is offline.
    Absent instances are excluded from source CTE → excluded from that day's
    bucket aggregate → slot_utilization and capacity_headroom change on those days.
    JVM memory grows from 22% → 48% of total over the year.
    """
    rng  = random.Random(44)
    rows = []

    story_start = date.fromisoformat(story["start_date"])
    story_end   = date.fromisoformat(story["end_date"])
    total_days  = max((story_end - story_start).days, 1)

    for d in date_range(story["start_date"], story["end_date"]):
        t = (d - story_start).days / total_days

        for sx in _SNAPLEXES:
            (inst_id, label, env, loc, _slots, _mem,
             _running, _down, _status, _reserved,
             _ts_start, _ts_end,
             node_count, cpu_cores, base_mem_gb,
             _online_from, _offline) = sx

            if not _is_online(sx, d):
                continue  # instance offline — no telemetry reported

            for n in range(node_count):
                node_id    = f"{inst_id}-node-{n + 1:02d}"
                node_label = f"{label} Node {n + 1}"

                total_mem  = round(base_mem_gb * rng.uniform(0.97, 1.03), 2)
                jvm_pct    = lerp(0.22, 0.48, t) + rng.uniform(-0.03, 0.03)
                jvm_mem    = round(total_mem * max(0.10, min(0.58, jvm_pct)), 2)
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
