# Remove everything vf/notebooks/dvi/insert.py added — and nothing else.
#
# Every DELETE is scoped by BOTH the org identifier AND
# record_inserted_by = 'seed-data-vf', so no other org's rows can be touched
# even if an org name were ever reused. demo-acme-direct, demo-acme-engineering,
# demo-meridian and every real Opsera org are untouched by construction.
#
# Run:
#   exec(open("/tmp/seed-data/vf/notebooks/dvi/delete.py").read())

import sys, os

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

from vf.generators import vf_gha_runs, vf_github_commits, vf_github_prs, vf_itsm
from vf.generators.story import load_entities

entities = load_entities()
org = entities["orgs"][0]["name"]

print(f"catalog : {CATALOG}")
print(f"org     : {org}  (scoped by org + record_inserted_by = 'seed-data-vf')")
print()

# Reverse of insert order.
for mod in (vf_gha_runs, vf_itsm, vf_github_prs, vf_github_commits):
    for stmt in mod.delete_sql(CATALOG, entities):
        print("  " + stmt)
        spark.sql(stmt)

print("\ndeleted")
