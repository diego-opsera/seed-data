"""
SQL + JSON emit helpers for the vf/ generators.

Three of the DVI source tables store their payload as a JSON *string* in a
single column and the ETL reads it back with get_json_object. Real prod rows are
huge (a raw PR payload is 18KB, a commit 47KB) because they are verbatim GitHub
REST responses — we emit only the paths the ETL actually reads, which keeps the
seed small and the intent legible.
"""
import hashlib
import json
import random


def sq(value) -> str:
    """SQL literal for a string/None, single quotes escaped."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def ts(value) -> str:
    """TIMESTAMP literal or NULL."""
    return "NULL" if value is None else f"TIMESTAMP '{value}'"


def dt(value) -> str:
    """DATE literal or NULL."""
    return "NULL" if value is None else f"DATE '{value}'"


def json_lit(obj) -> str:
    """Compact JSON as a SQL string literal."""
    return sq(json.dumps(obj, separators=(",", ":")))


def stable_seed(*parts) -> int:
    """
    Process-stable integer seed from arbitrary parts.

    NOT Python's hash(): str hashing is salted per process (PYTHONHASHSEED), so
    hash(('2026-09-01', 123)) differs between runs. The shared generators/ modules
    use hash() and are therefore non-reproducible across processes — visible here
    as row counts drifting 2082 -> 2076 -> 2055 on identical inputs. md5 keeps a
    re-seed byte-identical, which matters when delete.py + insert.py are re-run.
    """
    key = "\u0000".join(str(p) for p in parts).encode()
    return int.from_bytes(hashlib.md5(key).digest()[:8], "big") % (2**31)


def sha(seed: int) -> str:
    """Deterministic 40-char hex that looks like a git SHA."""
    return "".join(random.Random(seed).choices("0123456789abcdef", k=40))


def seeded(*parts) -> random.Random:
    """Stable RNG from arbitrary parts — same inputs always give the same output."""
    return random.Random(stable_seed(*parts))


def iso_z(day, hour=12, minute=0) -> str:
    """GitHub-style UTC timestamp: 2026-09-13T12:00:00Z."""
    return f"{day.isoformat()}T{hour:02d}:{minute:02d}:00Z"


def chunked_inserts(table: str, columns: str, value_lines: list[str],
                    *, batch: int = 500) -> list[str]:
    """
    Split into batched INSERTs. A single INSERT with tens of thousands of VALUES
    rows blows the Databricks SQL parser, so every generator batches.
    """
    out = []
    for i in range(0, len(value_lines), batch):
        rows = ",\n".join(value_lines[i:i + batch])
        out.append(f"INSERT INTO {table}\n  ({columns})\nVALUES\n{rows};")
    return out


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def arc_t(day, start, end) -> float:
    """Position of `day` along [start, end], clamped to 0..1."""
    total = max((end - start).days, 1)
    return max(0.0, min(1.0, (day - start).days / total))


def beat_for(day, start, story: dict) -> dict:
    """
    Narrative-beat multipliers for the month `day` falls in.

    Throughput and Impact self-normalize against their own P90, so a pure linear
    ramp flattens the trend to ~100 everywhere. narrative_beats in the story YAML
    keys month offsets from the arc start to per-dimension multipliers.
    """
    beats = story.get("narrative_beats") or {}
    if not beats:
        return {}
    offset = (day.year - start.year) * 12 + (day.month - start.month)
    return beats.get(offset) or beats.get(str(offset)) or {}
