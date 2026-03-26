"""
Shared helpers for data generation:
- Date range iteration
- Consistent random noise
- SQL STRUCT / ARRAY literal builders
- Internal consistency validators
"""
import random
from datetime import date, timedelta
from typing import Any


# ---------------------------------------------------------------------------
# US holidays 2025-2026 (major federal + commonly observed)
# ---------------------------------------------------------------------------

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
    """
    if "weekend_multiplier" not in story:
        return 1.0

    # Vacation periods: full shutdown
    for period in story.get("vacation_periods", []):
        vstart = date.fromisoformat(period["start"])
        vend = date.fromisoformat(period["end"])
        if vstart <= d <= vend:
            return 0.0

    day_seed = d.year * 10000 + d.month * 100 + d.day

    if d in US_HOLIDAYS:
        rng = random.Random(day_seed + 11)
        return rng.uniform(0.03, 0.08)

    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        emergency_str = story.get("emergency_weekend_start")
        if emergency_str:
            ew = date.fromisoformat(emergency_str)
            if d == ew or d == ew + timedelta(days=1):
                return story.get("emergency_weekend_multiplier", 0.25)
        rng = random.Random(day_seed + 33)
        return rng.uniform(0.05, 0.15)

    return 1.0


# ---------------------------------------------------------------------------
# User expansion and active user count
# ---------------------------------------------------------------------------

# IDE distribution for auto-generated users: 60% vscode, 30% intellij, 10% visualstudio
_IDE_POOL = ["vscode"] * 6 + ["intellij"] * 3 + ["visualstudio"] * 1


def default_ide(user_index: int) -> str:
    return _IDE_POOL[user_index % len(_IDE_POOL)]


def expand_users(entities: dict, story: dict) -> list[dict]:
    """
    Return the full user list for this story.
    Auto-generates users beyond the base entities list up to user_count_end.
    """
    base = entities["users"]
    target = story.get("user_count_end", len(base))
    if target <= len(base):
        return list(base)
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
    return users


def active_user_count(d: date, story: dict, total_users: int) -> int:
    """
    Return how many users are active on date d.
    - Vacation periods: 0
    - Weekends/holidays: random 0–10% of the day's normal weekday count
    - Weekdays: monthly trend base + monthly noise + daily jitter
    """
    # Vacation: no one
    for period in story.get("vacation_periods", []):
        if date.fromisoformat(period["start"]) <= d <= date.fromisoformat(period["end"]):
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
        emergency_str = story.get("emergency_weekend_start")
        if emergency_str:
            ew = date.fromisoformat(emergency_str)
            if d == ew or d == ew + timedelta(days=1):
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
