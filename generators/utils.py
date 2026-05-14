"""
Shared helpers for data generation:
- Date range iteration
- Consistent random noise
- SQL STRUCT / ARRAY literal builders
- Internal consistency validators
"""
import os
import random
from datetime import date, timedelta
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Story loader + narrative event windows
#
# Every narrative event (security spikes, incident week, hotfix recovery,
# vacation periods) is defined as a (start_offset, end_offset) tuple of
# days-before-today. load_story() materializes these to actual date sets
# anchored to today, so the demo always shows the same story arc relative
# to whenever it's run.
#
# US_HOLIDAYS stays calendar-absolute on purpose — federal holidays exist
# at fixed dates regardless of story window.
# ---------------------------------------------------------------------------

# (days_before_today_inclusive_max, days_before_today_inclusive_min)
#
# All Acme incident windows are anchored to ONE incident date — the production
# incident on Wed Mar 18 (offset 53). The security-alert spike, the
# suppressed-work week, and the hotfix recovery are all aligned around it.
#
# Today (2026-05-10) - 53 days = Mar 18 = dora.py _INC. Mirroring snapLogic.
EVENT_OFFSETS = {
    # Primary spike: alert volume rises Mon, peaks on incident Wed, tapers
    # through Sat. Spans Mar 16-21 (the incident week + the following Sat).
    "acme_primary_spike":   (55, 50),
    # Broader spike window for slow-rising metrics (Sonar / JUnit / GitCustodian
    # pass-rate dips). Encompasses the lead-in week + incident + hotfix.
    "acme_spike_broad":     (58, 46),
    # Earlier "warning" incident (~24 weeks before today). Smaller magnitude —
    # the company didn't fully address it, leading to the bigger SEV1 in March.
    "acme_secondary_spike": (174, 167),
    # Production incident week — suppressed activity (devs in war-room)
    "acme_incident_week":   (55, 51),
    # Hotfix recovery week after incident — surge of emergency commits/PRs
    "acme_hotfix_week":     (48, 46),
    # Vacation periods (~Thanksgiving wk + ~December break)
    "vacation_thanksgiving":(167, 163),
    "vacation_december":    (139, 130),
}

# Per-day spike volume profiles for code_scan_alert / secret_scan_alert /
# dependabot_scan_alert. Maps days-before-today → alerts-emitted-that-day.
# Peak on incident day (offset 53 = Mar 18 when run today=2026-05-10).
# Lead-in Mon-Tue, peak Wed (incident strikes), taper Thu-Sat.
ACME_CODE_SCAN_SPIKE_VOLUMES = {
    55: 4, 54: 6, 53: 10, 52: 8, 51: 6, 50: 3,         # incident week + Sat
    174: 2, 173: 5, 172: 3, 171: 2,                     # secondary spike
}
ACME_SECRET_SCAN_SPIKE_VOLUMES = {
    55: 2, 54: 3, 53: 5, 52: 4, 51: 3, 50: 1,
    174: 1, 173: 3, 172: 2, 171: 1,
}
ACME_DEPENDABOT_SPIKE_VOLUMES = {
    55: 3, 54: 4, 53: 7, 52: 6, 51: 4, 50: 2,
    174: 1, 173: 4, 172: 2, 171: 1,
}


def _materialize_window(end_date: date, offset_range: tuple) -> frozenset:
    """Build a frozenset of dates from end_date - max_offset..end_date - min_offset."""
    early, late = offset_range
    if early < late:
        early, late = late, early
    out = []
    d = end_date - timedelta(days=early)
    stop = end_date - timedelta(days=late)
    while d <= stop:
        out.append(d)
        d += timedelta(days=1)
    return frozenset(out)


def _materialize_volumes(end_date: date, volumes_by_offset: dict) -> dict:
    """Build {date: volume} from {days_before_today: volume}."""
    return {end_date - timedelta(days=offset): vol
            for offset, vol in volumes_by_offset.items()}


def materialize_events(end_date: date) -> dict:
    """Compute all narrative event date sets / volume maps for a given anchor."""
    return {
        name: _materialize_window(end_date, offsets)
        for name, offsets in EVENT_OFFSETS.items()
    } | {
        # Per-day volume tables (kept separate from the date sets above)
        "acme_code_scan_spike_volumes":   _materialize_volumes(end_date, ACME_CODE_SCAN_SPIKE_VOLUMES),
        "acme_secret_scan_spike_volumes": _materialize_volumes(end_date, ACME_SECRET_SCAN_SPIKE_VOLUMES),
        "acme_dependabot_spike_volumes":  _materialize_volumes(end_date, ACME_DEPENDABOT_SPIKE_VOLUMES),
    }


def load_story(name: str = "narrative", *, window_days: int = 365) -> dict:
    """Load a story YAML and inject today-relative dates + event windows.

    The on-disk YAML files don't carry start_date / end_date / event windows —
    they're computed here so the demo always shows the same arc relative to
    today. No insert script needs to mutate the file.

    Story dict gets:
      start_date / end_date — ISO strings, rolling window ending today
      events                — dict of frozen date sets + volume maps,
                              keyed by name (e.g. 'acme_primary_spike')
    """
    path = f"config/stories/{name}.yaml"
    if not os.path.isabs(path):
        for base in (".", "/tmp/seed-data"):
            candidate = os.path.join(base, path)
            if os.path.exists(candidate):
                path = candidate
                break
    with open(path) as f:
        story = yaml.safe_load(f)
    today = date.today()
    story["start_date"] = (today - timedelta(days=window_days)).isoformat()
    story["end_date"]   = today.isoformat()
    story["events"]     = materialize_events(today)
    return story


# ---------------------------------------------------------------------------
# US holidays 2025-2026 (major federal + commonly observed)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Production incident windows — anchored relative to today via EVENT_OFFSETS.
#
# Legacy callers that don't have a story dict still get correct behavior
# because we lazily materialize the windows against date.today() on first use.
# Newer callers can pass a story to use that story's anchor instead.
# ---------------------------------------------------------------------------

_LAZY_EVENTS_CACHE = {}


def _events_for(end_date: date) -> dict:
    if end_date not in _LAZY_EVENTS_CACHE:
        _LAZY_EVENTS_CACHE[end_date] = materialize_events(end_date)
    return _LAZY_EVENTS_CACHE[end_date]


def _events_from_story_or_today(story: dict | None) -> dict:
    if story and "events" in story:
        return story["events"]
    return _events_for(date.today())


def incident_multiplier(d: date, story: dict | None = None) -> float:
    """Activity multiplier for the production incident window.
    Incident week: 20% of normal throughput (devs in war-room, not shipping).
    Recovery week: 145% as hotfixes and catch-up work surge.

    `story` is optional for backward compat — if not provided, anchors to today.
    """
    events = _events_from_story_or_today(story)
    if d in events["acme_incident_week"]:
        return 0.20
    if d in events["acme_hotfix_week"]:
        return 1.45
    return 1.0


def is_incident_suppressed(d: date, story: dict | None = None) -> bool:
    return d in _events_from_story_or_today(story)["acme_incident_week"]


def is_incident_hotfix(d: date, story: dict | None = None) -> bool:
    return d in _events_from_story_or_today(story)["acme_hotfix_week"]


US_HOLIDAYS = frozenset({
    date(2025, 5, 26),   # Memorial Day
    date(2025, 7, 4),    # Independence Day
    date(2025, 9, 1),    # Labor Day
    date(2025, 11, 11),  # Veterans Day
    date(2025, 11, 27),  # Thanksgiving
    date(2025, 11, 28),  # Day after Thanksgiving
    date(2025, 12, 24),  # Christmas Eve
    date(2025, 12, 25),  # Christmas
    date(2025, 12, 26),  # Day after Christmas
    date(2026, 1, 1),    # New Year's Day
    date(2026, 1, 19),   # MLK Day
    date(2026, 2, 16),   # Presidents Day
})


# ---------------------------------------------------------------------------
# Trend / day scaling
# ---------------------------------------------------------------------------

def lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation between a and b at position t (0..1)."""
    return a + (b - a) * t


def trend_base(story: dict, d: date) -> dict:
    """
    Return per_user_per_day values interpolated over the date range.
    Falls back to static per_user_per_day if no start/end values provided.
    """
    if "per_user_per_day_start" not in story:
        return story["per_user_per_day"]
    start = date.fromisoformat(story["start_date"])
    end = date.fromisoformat(story["end_date"])
    total_days = max((end - start).days, 1)
    t = max(0.0, min(1.0, (d - start).days / total_days))
    s = story["per_user_per_day_start"]
    e = story["per_user_per_day_end"]
    return {k: max(1, round(lerp(s[k], e[k], t))) for k in s}


def day_scale(d: date, story: dict) -> float:
    """
    Return a 0.0–1.0 activity multiplier for a given date.
    Returns 1.0 for all days if the story has no day-scaling config.
    Vacation periods return 0.0. Weekends/holidays return a small random scale.
    Vacation/emergency-weekend dates come from story["events"] (materialized
    from utils.EVENT_OFFSETS at story-load time).
    """
    if "weekend_multiplier" not in story:
        return 1.0

    events = _events_from_story_or_today(story)

    # Vacation periods (Thanksgiving week + December break) → full shutdown
    if d in events["vacation_thanksgiving"] or d in events["vacation_december"]:
        return 0.0

    day_seed = d.year * 10000 + d.month * 100 + d.day

    if d in US_HOLIDAYS:
        rng = random.Random(day_seed + 11)
        return rng.uniform(0.03, 0.08)

    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        # SEV1 emergency weekend → elevated activity (devs on-call)
        if d in events["acme_primary_spike"]:
            return story.get("emergency_weekend_multiplier", 0.25)
        rng = random.Random(day_seed + 33)
        return rng.uniform(0.05, 0.15)

    return 1.0


# ---------------------------------------------------------------------------
# User expansion and active user count
# ---------------------------------------------------------------------------

# TODO(known-bug): The "Developer Language And Editor Usage" dashboard table still shows
# near-uniform user counts and ~45% acceptance rate across all languages/features/models.
# Root cause suspected: Databricks cluster caches sys.modules across runs, so even with the
# module-flush in notebook_insert.py, the weighted language assignment in expand_users()
# and per-language LANG_ACCEPTANCE_RATES may not be taking effect.
# The dashboard "Percentage" column appears to show acceptance rate (accepted/generated),
# not language share, and it averages to ~45% regardless of per-language rates set below.
# Not blocking for demo but worth a proper investigation (cluster restart + debug logging).

# Language weights based on opentelemetry-demo repo distribution (normalized, no Dockerfile/Other)
_LANG_WEIGHTS = {
    "typescript": 401,
    "python": 195,
    "elixir": 108,
    "go":  64,
    "csharp": 43,
}

# Per-language Copilot acceptance rates (TypeScript/Python higher, Elixir lower due to less training data)
LANG_ACCEPTANCE_RATES = {
    "typescript": 0.52,
    "python": 0.55,
    "elixir": 0.38,
    "go": 0.45,
    "csharp": 0.48,
}

# IDE weights: VSCode dominates TypeScript/Python/Go; Visual Studio for C#; IntelliJ general
_IDE_WEIGHTS = {
    "vscode": 70,
    "intellij": 15,
    "visualstudio": 10,
    "eclipse": 3,
    "xcode": 2,
}


def default_ide(user_index: int) -> str:
    # Kept for backward compatibility; expand_users now assigns ide directly
    pool = ["vscode"] * 7 + ["intellij"] * 2 + ["visualstudio"] * 1
    return pool[user_index % len(pool)]


def expand_users(entities: dict, story: dict) -> list[dict]:
    """
    Return the full user list for this story.
    Auto-generates users beyond the base entities list up to user_count_end.
    Each user gets a stable language and ide assigned once (seeded by user id).
    Named users in user_ide_map get their ide overridden.
    """
    base = entities["users"]
    target = story.get("user_count_end", len(base))
    user_ide_map = story.get("user_ide_map", {})

    languages = entities["languages"]
    ides = entities["ides"]
    lang_weights = [_LANG_WEIGHTS.get(l, 10) for l in languages]
    ide_weights = [_IDE_WEIGHTS.get(ide, 5) for ide in ides]

    if target <= len(base):
        users = list(base)
    else:
        users = list(base)
        for i in range(len(base), target):
            n = i + 1
            num = str(n).zfill(3)
            users.append({
                "id": 9990000 + n,
                "login": f"demo-user-{num}",
                "assignee_login": f"demo-user-{num}",
                "team": "demo-backend" if n % 3 != 0 else "demo-frontend",
            })

    for user in users:
        rng = random.Random(user["id"])
        user["language"] = rng.choices(languages, weights=lang_weights)[0]
        user["ide"] = user_ide_map.get(user["login"]) or rng.choices(ides, weights=ide_weights)[0]

    return users


def active_user_count(d: date, story: dict, total_users: int) -> int:
    """
    Return how many users are active on date d.
    - Vacation periods: 0
    - Weekends/holidays: random 0–10% of the day's normal weekday count
    - Weekdays: monthly trend base + monthly noise + daily jitter
    """
    events = _events_from_story_or_today(story)

    # Vacation: no one
    if d in events["vacation_thanksgiving"] or d in events["vacation_december"]:
        return 0

    # Compute the weekday baseline for this date
    if "user_count_start" not in story:
        weekday_base = total_users
    else:
        start = date.fromisoformat(story["start_date"])
        end = date.fromisoformat(story["end_date"])
        total_days = max((end - start).days, 1)
        month_anchor = date(d.year, d.month, 1)
        t = max(0.0, min(1.0, (month_anchor - start).days / total_days))
        base_count = lerp(story["user_count_start"], story["user_count_end"], t)
        monthly_noise = story.get("user_count_noise_pct", 0)
        monthly_seed = d.year * 100 + d.month
        weekday_base = max(1, min(jitter(round(base_count), monthly_noise, monthly_seed), total_users))

    day_seed = d.year * 10000 + d.month * 100 + d.day

    # Weekends/holidays: 0–10% of weekday base, random per day
    if d.weekday() >= 5 or d in US_HOLIDAYS:
        # SEV1 emergency weekend (anchored to today via story["events"]) →
        # elevated weekend activity from on-call response.
        if d in events["acme_primary_spike"]:
            frac = story.get("emergency_weekend_multiplier", 0.25)
            return max(1, round(weekday_base * frac))
        rng = random.Random(day_seed + 77)
        frac = rng.uniform(0.0, 0.10)
        return max(0, round(weekday_base * frac))

    # Regular weekday: absolute Gaussian noise so every bar looks different
    daily_abs_noise = story.get("user_count_daily_abs_noise", 8)
    rng = random.Random(day_seed)
    offset = round(rng.gauss(0, daily_abs_noise / 2))
    return max(1, min(weekday_base + offset, total_users))


# ---------------------------------------------------------------------------
# Smooth duration helper — for charts that show per-event time-to-X
# (cycle time, time to PR, MTTR, CR resolve time, pipeline duration, ...).
#
# Problem: generators that drew duration with rng.randint(a, b) per event
# produced charts that look like white noise — day-over-day averages swing
# wildly because each event independently rolls a fresh number.
#
# Pattern: instead of rolling a fresh number per event, compute one smooth
# per-day target that drifts slowly week-to-week, then optionally add tiny
# per-event jitter on top. Result: the baseline is a clean band so the
# incident week's widening / hotfix week's narrowing actually stands out.
# ---------------------------------------------------------------------------

def smooth_duration_days(
    d: date,
    base_days: float,
    *,
    incident_widen: float = 2.5,
    hotfix_narrow: float = 0.20,
    weekly_drift_pct: float = 0.10,
    daily_jitter_pct: float = 0.08,
    seed_key: tuple = (),
    story: dict | None = None,
) -> float:
    """Per-day duration target (in days) that drifts smoothly day to day.

    Deterministic — same (d, base_days, seed_key) → same number.

    Knobs:
      base_days        baseline target for a normal day
      weekly_drift_pct ±% the weekly anchor drifts off base (default 10%)
      daily_jitter_pct ±% Gaussian jitter added per day (default 8%)
      incident_widen   multiplier during incident week (default 2.5×)
                       — story: in-flight work stalls while devs firefight
      hotfix_narrow    multiplier during hotfix week (default 0.20×)
                       — story: surgical hotfixes ship same-day

    seed_key differentiates parallel streams (e.g., ('copilot',) vs
    ('non_copilot',)) so they get distinct curves on the same chart.
    Result is clamped to ≥ 0.05 days.
    """
    iso = d.isocalendar()
    week_rng = random.Random(hash((iso[0], iso[1], "wk") + seed_key) % (2**31))
    weekly_factor = 1.0 + week_rng.uniform(-weekly_drift_pct, weekly_drift_pct)

    day_rng = random.Random(hash((d.toordinal(), "dy") + seed_key) % (2**31))
    daily_factor = 1.0 + day_rng.gauss(0, daily_jitter_pct)

    target = base_days * weekly_factor * daily_factor

    if is_incident_suppressed(d, story):
        target *= incident_widen
    elif is_incident_hotfix(d, story):
        target *= hotfix_narrow

    return max(0.05, target)


def smooth_duration_hours(d: date, base_hours: float, **kw) -> float:
    """Same as smooth_duration_days but in hours. Default knobs unchanged."""
    return smooth_duration_days(d, base_hours / 24.0, **kw) * 24.0


# ---------------------------------------------------------------------------
# AI tool roster — multi-tool comparison (notebooks/ai_compare/)
# ---------------------------------------------------------------------------

def tool_rollout_date(tool: dict, story: dict) -> date:
    """Date this tool becomes available within the story window."""
    return date.fromisoformat(story["start_date"]) + timedelta(
        days=int(tool.get("rollout_offset_days", 0))
    )


def tool_is_live(tool: dict, d: date, story: dict) -> bool:
    """True iff this tool has been rolled out by date d."""
    return d >= tool_rollout_date(tool, story)


def tool_allocation_on(tool: dict, d: date, story: dict) -> int:
    """Allocated licenses for this tool on date d.

    Ramps linearly from 0 over a 30-day rollout once the tool is live, capped
    at tool['allocation']. Pre-rollout returns 0.
    """
    start = tool_rollout_date(tool, story)
    if d < start:
        return 0
    ramp_days = 30
    days_in = (d - start).days
    if days_in >= ramp_days:
        return int(tool["allocation"])
    return max(0, round(int(tool["allocation"]) * (days_in + 1) / ramp_days))


def assign_users_to_tool(users: list[dict], tool: dict, seed: int = 0) -> list[dict]:
    """Deterministically pick the subset of users licensed for this tool.

    Copilot covers everyone (allocation=100 with 100 users). Smaller tools
    pick a stable subset — higher-id users (later joiners) are more likely
    to be on the newer tools, mirroring real adoption patterns.
    """
    n = min(int(tool["allocation"]), len(users))
    if n >= len(users):
        return list(users)
    rng = random.Random(seed + abs(hash(tool["name"])))
    return rng.sample(users, n)


def tool_active_users(tool: dict, d: date, story: dict, total_assigned: int) -> int:
    """Active users for this tool on date d.

    active = min(allocated_today, total_assigned) * active_share * day_scale(d)
    """
    if not tool_is_live(tool, d, story):
        return 0
    allocated = min(tool_allocation_on(tool, d, story), total_assigned)
    if allocated == 0:
        return 0
    scale = day_scale(d, story)
    if scale == 0.0:
        return 0
    return max(0, round(allocated * float(tool["active_share"]) * scale))


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def date_range(start: str, end: str):
    """Yield date objects from start to end inclusive."""
    current = date.fromisoformat(start)
    stop = date.fromisoformat(end)
    while current <= stop:
        yield current
        current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Noise
# ---------------------------------------------------------------------------

def jitter(value: int, pct: float, seed: int = None) -> int:
    """Apply ±pct% noise to value. Returns a non-negative int."""
    if pct == 0:
        return value
    rng = random.Random(seed)
    factor = 1 + rng.uniform(-pct / 100, pct / 100)
    return max(0, round(value * factor))


# ---------------------------------------------------------------------------
# Consistency math
# ---------------------------------------------------------------------------

def split_across(total: int, n: int) -> list[int]:
    """
    Divide total into n non-negative integers that sum to total.
    Splits as evenly as possible with remainder distributed to first buckets.
    """
    if n <= 0:
        return []
    base, remainder = divmod(total, n)
    return [base + (1 if i < remainder else 0) for i in range(n)]


def acceptance_subset(generated: int, acceptance_rate: float) -> int:
    """Return accepted count that is <= generated and respects the rate."""
    return min(generated, round(generated * acceptance_rate))


# ---------------------------------------------------------------------------
# SQL literal builders
# ---------------------------------------------------------------------------

class RawSQL:
    """Wrap a pre-built SQL expression so _sql_val passes it through unquoted."""
    def __init__(self, expr: str):
        self.expr = expr


def _sql_val(v: Any) -> str:
    """Convert a Python value to its SQL literal representation."""
    if v is None:
        return "NULL"
    if isinstance(v, RawSQL):
        return v.expr
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        escaped = v.replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(v, date):
        return f"DATE '{v.isoformat()}'"
    raise TypeError(f"Unsupported type for SQL literal: {type(v)}")


def named_struct(fields: dict[str, Any]) -> str:
    """
    Build a Databricks SQL NAMED_STRUCT literal.

    named_struct({"feature": "code_completion", "loc_added_sum": 10})
    → NAMED_STRUCT('feature', 'code_completion', 'loc_added_sum', 10)
    """
    parts = []
    for k, v in fields.items():
        parts.append(f"'{k}'")
        parts.append(_sql_val(v))
    return f"NAMED_STRUCT({', '.join(parts)})"


def sql_array(structs: list[str]) -> str:
    """
    Wrap a list of NAMED_STRUCT strings into an ARRAY literal.
    Pass an empty list to produce ARRAY().
    """
    return f"ARRAY({', '.join(structs)})"


# ---------------------------------------------------------------------------
# Nested array builders
# These produce the ARRAY<STRUCT<...>> literals used in enterprise/user tables.
# Each builder takes the minimal inputs needed and derives the rest.
# ---------------------------------------------------------------------------

# Note: loc_added_sum, loc_deleted_sum, loc_suggested_to_add_sum, loc_suggested_to_delete_sum
# are NULL in real nested array data — we match that behavior here.

def totals_by_feature_entry(
    feature: str,
    code_gen: int,
    code_accept: int,
    interaction_count: int,
    accepted_loc: int,
    generated_loc: int,
) -> str:
    return named_struct({
        "accepted_loc_sum": accepted_loc,
        "code_acceptance_activity_count": code_accept,
        "code_generation_activity_count": code_gen,
        "feature": feature,
        "generated_loc_sum": generated_loc,
        "loc_added_sum": None,
        "loc_deleted_sum": None,
        "loc_suggested_to_add_sum": None,
        "loc_suggested_to_delete_sum": None,
        "user_initiated_interaction_count": interaction_count,
    })


# Plugin names per IDE, confirmed from real data (vscode = "copilot-chat")
PLUGIN_NAMES = {
    "vscode":       "copilot-chat",
    "intellij":     "copilot",
    "visualstudio": "copilot",
    "eclipse":      "copilot",
    "xcode":        "copilot",
}

PLUGIN_VERSIONS = {
    "vscode":       "0.28.5",
    "intellij":     "1.5.10.6",
    "visualstudio": "0.2.1139.0",
    "eclipse":      "1.0.0",
    "xcode":        "1.0.0",
}


def totals_by_ide_entry(
    ide: str,
    code_gen: int,
    code_accept: int,
    interaction_count: int,
    accepted_loc: int,
    generated_loc: int,
    sampled_at: str,
) -> str:
    plugin_struct = RawSQL(named_struct({
        "plugin": PLUGIN_NAMES.get(ide, "copilot"),
        "plugin_version": PLUGIN_VERSIONS.get(ide, "1.0.0"),
        "sampled_at": sampled_at,
    }))
    return named_struct({
        "accepted_loc_sum": accepted_loc,
        "code_acceptance_activity_count": code_accept,
        "code_generation_activity_count": code_gen,
        "generated_loc_sum": generated_loc,
        "ide": ide,
        "last_known_plugin_version": plugin_struct,
        "loc_added_sum": None,
        "loc_deleted_sum": None,
        "loc_suggested_to_add_sum": None,
        "loc_suggested_to_delete_sum": None,
        "user_initiated_interaction_count": interaction_count,
    })


def totals_by_ide_entry_enterprise(
    ide: str,
    code_gen: int,
    code_accept: int,
    interaction_count: int,
    accepted_loc: int,
    generated_loc: int,
) -> str:
    """IDE entry for enterprise_level_copilot_metrics — no plugin version field."""
    return named_struct({
        "accepted_loc_sum": accepted_loc,
        "code_acceptance_activity_count": code_accept,
        "code_generation_activity_count": code_gen,
        "generated_loc_sum": generated_loc,
        "ide": ide,
        "loc_added_sum": None,
        "loc_deleted_sum": None,
        "loc_suggested_to_add_sum": None,
        "loc_suggested_to_delete_sum": None,
        "user_initiated_interaction_count": interaction_count,
    })


def totals_by_language_feature_entry(
    language: str,
    feature: str,
    code_gen: int,
    code_accept: int,
    accepted_loc: int,
    generated_loc: int,
) -> str:
    return named_struct({
        "accepted_loc_sum": accepted_loc,
        "code_acceptance_activity_count": code_accept,
        "code_generation_activity_count": code_gen,
        "feature": feature,
        "generated_loc_sum": generated_loc,
        "language": language,
        "loc_added_sum": None,
        "loc_deleted_sum": None,
        "loc_suggested_to_add_sum": None,
        "loc_suggested_to_delete_sum": None,
    })


def totals_by_language_model_entry(
    language: str,
    model: str,
    code_gen: int,
    code_accept: int,
    accepted_loc: int,
    generated_loc: int,
) -> str:
    return named_struct({
        "accepted_loc_sum": accepted_loc,
        "code_acceptance_activity_count": code_accept,
        "code_generation_activity_count": code_gen,
        "generated_loc_sum": generated_loc,
        "language": language,
        "loc_added_sum": None,
        "loc_deleted_sum": None,
        "loc_suggested_to_add_sum": None,
        "loc_suggested_to_delete_sum": None,
        "model": model,
    })


def totals_by_model_feature_entry(
    model: str,
    feature: str,
    code_gen: int,
    code_accept: int,
    interaction_count: int,
    accepted_loc: int,
    generated_loc: int,
) -> str:
    return named_struct({
        "accepted_loc_sum": accepted_loc,
        "code_acceptance_activity_count": code_accept,
        "code_generation_activity_count": code_gen,
        "feature": feature,
        "generated_loc_sum": generated_loc,
        "loc_added_sum": None,
        "loc_deleted_sum": None,
        "loc_suggested_to_add_sum": None,
        "loc_suggested_to_delete_sum": None,
        "model": model,
        "user_initiated_interaction_count": interaction_count,
    })


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

def validate_row(row: dict, table: str):
    """
    Raise AssertionError if a generated row violates consistency rules.
    Call this on every generated row before building SQL.
    """
    gen = row.get("code_generation_activity_count", 0)
    acc = row.get("code_acceptance_activity_count", 0)
    assert acc <= gen, (
        f"[{table}] acceptance ({acc}) > generation ({gen})"
    )

    sugg_add = row.get("loc_suggested_to_add_sum", 0)
    loc_add = row.get("loc_added_sum", 0)
    assert loc_add <= sugg_add, (
        f"[{table}] loc_added ({loc_add}) > loc_suggested_to_add ({sugg_add})"
    )

    sugg_del = row.get("loc_suggested_to_delete_sum", 0)
    loc_del = row.get("loc_deleted_sum", 0)
    assert loc_del <= sugg_del, (
        f"[{table}] loc_deleted ({loc_del}) > loc_suggested_to_delete ({sugg_del})"
    )
