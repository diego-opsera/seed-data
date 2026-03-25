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
