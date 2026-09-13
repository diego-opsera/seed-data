# Generate the SQL without executing it — no Spark, no Databricks.
#
# Catches the errors that are expensive to find on a cluster: malformed JSON
# payloads, unescaped quotes, column/value count mismatches, and windows that
# produce zero rows. Also asserts the properties the DVI ETL actually requires,
# so a generator regression shows up here rather than as a silently dark tile.
#
# Run either way:
#   python3 vf/notebooks/dvi/dryrun.py
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/dryrun.py").read())

import json
import os
import re
import sys
from datetime import date, datetime, timedelta

for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key.startswith("vf") or _key in ("loader",):
        del sys.modules[_key]

_CANDIDATES = ["/tmp/seed-data", os.path.expanduser("~/seed-data"), os.getcwd()]
REPO_ROOT = next(
    (c for c in _CANDIDATES if os.path.exists(os.path.join(c, "vf", "config", "entities.yaml"))),
    None,
)
if REPO_ROOT is None:
    raise RuntimeError("could not locate the seed-data checkout")
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

CATALOG = "playground_prod"

from vf.generators import vf_gha_runs, vf_github_commits, vf_github_prs, vf_itsm
from vf.generators.story import load_dvi_story, load_entities

entities = load_entities()
story = load_dvi_story()
roster_emails = {u["email"] for u in entities["users"]}

today = date.today()
lo = today.replace(day=1).isoformat()
nxt = (today.replace(day=28) + timedelta(days=4)).replace(day=1)
hi = (nxt - timedelta(days=1)).isoformat()

print(f"dry run — smoke window {lo} → {hi}\n")


def _created_on_or_before(line: str, today_iso: str) -> bool:
    """First DATE literal on an itsm VALUES row is issue_created_date."""
    m = re.search(r"DATE '(\d{4}-\d{2}-\d{2})'", line)
    return bool(m) and m.group(1) <= today_iso


failures = []


def check(cond, msg):
    if cond:
        print(f"  PASS  {msg}")
    else:
        print(f"  FAIL  {msg}")
        failures.append(msg)


for label, mod in [
    ("commits", vf_github_commits),
    ("prs", vf_github_prs),
    ("itsm", vf_itsm),
    ("gha_runs", vf_gha_runs),
]:
    stmts = mod.generate(CATALOG, entities, story, date_from=lo, date_to=hi)
    rows = sum(s.count("\n  (") for s in stmts)
    print(f"### {label}: {len(stmts)} statements, ~{rows} rows")
    check(rows > 0, f"{label} produced rows")
    check(all(s.rstrip().endswith(";") for s in stmts), f"{label} statements terminated")
    check(all(f"{CATALOG}." in s for s in stmts), f"{label} targets {CATALOG} only")

    blob = "\n".join(stmts)
    # Every JSON payload must parse, and carry the email the ETL joins on.
    payloads = re.findall(r"'(\{\"(?:[^']|'')*?\})'", blob)
    if payloads:
        parsed, bad = 0, 0
        emails = set()
        for p in payloads[:400]:
            try:
                obj = json.loads(p.replace("''", "'"))
                parsed += 1
                e = (obj.get("commit", {}).get("author", {}) or {}).get("email")
                if e:
                    emails.add(e)
            except Exception:
                bad += 1
        check(bad == 0, f"{label} JSON payloads parse ({parsed} checked)")
        if emails:
            check(emails <= roster_emails,
                  f"{label} payload emails all come from the roster ({len(emails)} distinct)")
    if label == "itsm":
        check("HIGHEST" in blob or "BLOCKER" in blob,
              "itsm emits HIGHEST/BLOCKER — Quality counts only those")
        check(any(e in blob for e in roster_emails), "itsm carries roster assignee_email")
        # Every developer needs issues, or per-dev Quality/Impact is noise.
        covered = {e for e in roster_emails if e in blob}
        check(len(covered) == len(roster_emails),
              f"itsm covers all {len(roster_emails)} developers (got {len(covered)})")
        # The ETL window ends TODAY, so rows dated after today are not counted
        # yet. Enough defects must land on or before today for Quality to work.
        today_iso = date.today().isoformat()
        in_window = [l for l in blob.splitlines()
                     if l.startswith("  ('jira'") and _created_on_or_before(l, today_iso)]
        defects_in_window = [l for l in in_window if "'bug'" in l
                             and ("HIGHEST" in l or "BLOCKER" in l)]
        check(len(defects_in_window) >= 5,
              f"at least 5 high-priority defects created on or before today "
              f"(got {len(defects_in_window)}; rows after today are invisible "
              f"until their date arrives)")
        print(f"  INFO  itsm rows created <= today: {len(in_window) // 2} "
              f"of {blob.count(chr(10) + '  (') // 2}")
    if label == "gha_runs":
        check("record_inserted_by" in stmts[0], "gha_runs sets record_inserted_by")
        check("TIMESTAMP" in blob, "gha_runs populates record_insert_datetime (ETL filters on it)")
    print()

# ── predicted tiles from the generated rows ─────────────────────────────────
# The point of doing this locally: three of the five tiles saturate at trivial
# volumes (Throughput excellent = 20 PRs, Impact excellent = 12 features) and
# Velocity reads 'critical' above 24h. Catching that here beats discovering it
# after a seed + ETL cycle.
def _interp(x, x0, x1, y0, y1):
    if x1 == x0:
        return y0
    return max(0.0, min(100.0, y0 + (x - x0) / (x1 - x0) * (y1 - y0)))


def _norm(raw, exc, good, ni, inverted):
    if inverted:
        if raw <= exc:
            return 100.0
        if raw <= good:
            return _interp(raw, exc, good, 100, 75)
        if raw <= ni:
            return _interp(raw, good, ni, 75, 50)
        dc = ni + (ni - exc)
        return 0.0 if raw >= dc else _interp(raw, ni, dc, 50, 0)
    if raw >= exc:
        return 100.0
    if raw >= good:
        return _interp(raw, good, exc, 75, 100)
    if raw >= ni:
        return _interp(raw, ni, good, 50, 75)
    return 0.0 if raw <= 0 else _interp(raw, 0, ni, 0, 50)


pr_stmts = vf_github_prs.generate(CATALOG, entities, story, date_from=lo, date_to=hi)
pr_payloads = re.findall(r"'(\{\"id\":(?:[^']|'')*?\})'", "\n".join(pr_stmts))
cycles = []
for p_ in pr_payloads:
    try:
        o = json.loads(p_.replace("''", "'"))
        c = datetime.strptime(o["created_at"], "%Y-%m-%dT%H:%M:%SZ")
        m = datetime.strptime(o["merged_at"], "%Y-%m-%dT%H:%M:%SZ")
        cycles.append((m - c).total_seconds() / 3600.0)
    except Exception:
        pass
cycles.sort()
median_cycle = cycles[len(cycles) // 2] if cycles else 0.0
merged_prs = len(cycles)

itsm_blob = "\n".join(vf_itsm.generate(CATALOG, entities, story, date_from=lo, date_to=hi))
# Halve: the generator writes the same rows to _hist and _current.
itsm_rows = itsm_blob.count("\n  (") // 2
feature_types = ("'story'", "'epic'", "'feature'", "'enhancement'")
resolved_features = 0
for line in itsm_blob.splitlines():
    if not line.startswith("  ('jira'"):
        continue
    if any(ft in line for ft in feature_types) and "TIMESTAMP" in line:
        # resolved rows carry a non-NULL issue_resolution_date (3rd timestamp)
        if line.count("TIMESTAMP") >= 5:
            resolved_features += 1
resolved_features //= 2

print("### predicted tiles (snapshot path, this window)")
tiles = {
    "velocity":   {"raw": round(median_cycle, 2), "unit": "median cycle hours",
                   "score": round(_norm(median_cycle, 4, 8, 24, True), 1)},
    "throughput": {"raw": merged_prs, "unit": "merged PRs",
                   "score": round(_norm(merged_prs, 20, 15, 8, False), 1)},
    "impact":     {"raw": resolved_features, "unit": "resolved features",
                   "score": round(_norm(resolved_features, 12, 8, 3, False), 1)},
}
print(json.dumps(tiles, indent=2))
check(cycles and 8 < median_cycle < 44,
      f"velocity median {median_cycle:.1f}h is on the scoring curve (8-44h), not saturated or dead")
for name in ("throughput", "impact"):
    if tiles[name]["score"] >= 100:
        print(f"  NOTE  {name} saturates at 100 ({tiles[name]['raw']} {tiles[name]['unit']}) — "
              f"expected for a COUNT metric; per-developer path carries the spread")
print()

print("### sample statement (first 900 chars of the commits INSERT)")
print(vf_github_commits.generate(CATALOG, entities, story, date_from=lo, date_to=hi)[0][:900])

print()
if failures:
    print(f"{len(failures)} CHECK(S) FAILED:")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
print("all checks passed — safe to run insert.py")
