# Seed demo-acme-vf DVI data into playground_prod.
#
# CATALOG IS ALWAYS playground_prod. Every statement is INSERT-only and scoped to
# org 'demo-acme-vf' with record_inserted_by = 'seed-data-vf', so delete.py can
# remove exactly what this added and no other org's rows are ever touched.
#
# Modes — set VF_MODE before exec(), default 'smoke':
#   smoke : current calendar month only. The Phase 2 checkpoint — proves the
#           cross-source email join resolves into individuals[] before we build
#           the full arc.
#   full  : the whole story arc (2025-09 → ~3.5 months past today). Forward-dated
#           on purpose: playground_prod is a one-time prod copy cut off
#           ~2026-08-02 and the DVI snapshot falls back only one month, so
#           without future rows Velocity and Throughput zero out on 2026-10-01.
#
# Run:
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/insert.py").read())
#   VF_MODE = "full"; exec(open("/tmp/seed-data/vf/notebooks/dvi/insert.py").read())
#
# Nothing shows in the UI until the ETL materializes vf_dvi. No manual trigger
# needed — the backend scheduler runs syncAllConceptViews (incl. syncDvi) 30s
# after boot and daily at ETL_SYNC_HOURS (default 07:00 / 19:00 UTC).

import sys, os
from datetime import date

for _key in list(sys.modules.keys()):
    if _key.startswith("generators") or _key.startswith("vf") or _key in ("loader",):
        del sys.modules[_key]

_CANDIDATES = ["/tmp/seed-data", os.path.expanduser("~/seed-data"), os.getcwd()]
REPO_ROOT = next(
    (c for c in _CANDIDATES if os.path.exists(os.path.join(c, "vf", "config", "entities.yaml"))),
    None,
)
if REPO_ROOT is None:
    raise RuntimeError("could not locate the seed-data checkout — tried: " + ", ".join(_CANDIDATES))
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

CATALOG = "playground_prod"
VF_MODE = globals().get("VF_MODE", "smoke")

from vf.generators import vf_gha_runs, vf_github_commits, vf_github_prs, vf_itsm
from vf.generators.story import load_dvi_story, load_entities

entities = load_entities()
story = load_dvi_story()
org = entities["orgs"][0]["name"]

if VF_MODE == "smoke":
    today = date.today()
    lo = today.replace(day=1).isoformat()
    nxt = (today.replace(day=28) + __import__("datetime").timedelta(days=4)).replace(day=1)
    hi = (nxt - __import__("datetime").timedelta(days=1)).isoformat()
elif VF_MODE == "full":
    lo, hi = story["start_date"], story["end_date"]
else:
    raise ValueError(f"VF_MODE must be 'smoke' or 'full', got {VF_MODE!r}")

print(f"catalog : {CATALOG}")
print(f"org     : {org}")
print(f"mode    : {VF_MODE}")
print(f"window  : {lo} → {hi}")
print()

GENERATORS = [
    ("commits  (raw_github_commits_rest_api)", vf_github_commits),
    ("prs      (raw_github_pull_requests_rest_api_prs)", vf_github_prs),
    ("itsm     (mt_itsm_issues_hist + _current)", vf_itsm),
    ("gha runs (github_actions_runs_rest_api)", vf_gha_runs),
]

total_stmts = 0
for label, mod in GENERATORS:
    stmts = mod.generate(CATALOG, entities, story, date_from=lo, date_to=hi)
    n_rows = sum(s.count("\n  (") for s in stmts)
    print(f"  {label}: {len(stmts)} statements, ~{n_rows} rows")
    for s in stmts:
        spark.sql(s)
    total_stmts += len(stmts)

print(f"\ninserted via {total_stmts} statements")
print("\nNext:")
print("  1. exec(open('/tmp/seed-data/vf/notebooks/dvi/verify_scoped.py').read())")
print("     — confirms the Databricks side satisfies the ETL's own predicates")
print("  2. wait for the scheduled ETL (07:00 / 19:00 UTC), then check the DVI dashboard")
