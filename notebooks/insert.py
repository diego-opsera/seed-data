# ============================================================
# MASTER INSERT — seeds all demo-acme data in dependency order
# Catalog: playground_prod (see KNOWN LIMITATIONS in MASTER_SCRIPTS_PLAN.md)
# Run from Databricks notebook cell:
#   exec(open("/tmp/seed-data/notebooks/insert.py").read())
# ============================================================

BASE = "/tmp/seed-data/notebooks"

# Story dates are computed at runtime by generators.utils.load_story() — see
# config/stories/narrative.yaml. This script no longer rewrites the YAML.

def run(path):
    print(f"\n{'='*60}")
    print(f"  RUNNING: {path}")
    print(f"{'='*60}\n")
    exec(open(f"{BASE}/{path}").read(), globals())

# 1. core — enterprise copilot metrics (enterprise_id=999999)
run("core/insert.py")

# 2. direct — copilot data (org=demo-acme-direct): direct_data, seats, billing, ai metrics
run("direct/insert.py")

# 3. dora — pipeline events, sdm tables, DORA/CTFC filter group
#    Must run before devex/ so the shared filter group exists for devex to attach to
run("dora/insert.py")

# 4. devex — devex data: pull_requests, commits, teams, itsm
#    Attaches devex filter values to the filter group created by dora/
#    (CTFC itsm dependency is at API query time, not insert time)
run("devex/insert.py")

# 5. release_mgmt — release management (fix_version LIKE 'demo-%')
run("release_mgmt/insert.py")

# 6. snaplogic — SnapLogic integration data (org=demo-acme-direct)
run("snaplogic/insert.py")

# 7. value_stream — Issue Stream / Flow View denormalized fact table
#    Self-contained — generates fresh data, no dependency on other batches' tables.
#    Seeds rows for BOTH demo-acme-direct and demo-meridian.
run("value_stream/insert.py")

# 8. code_reliability — dashboard at /insights/v2/code-reliability
#    Self-contained, seeds BOTH orgs in one place. Add new generators here as
#    we build them (sonar, twistlock, was, etc.).
run("code_reliability/insert.py")

print("\n" + "="*60)
print("  MASTER INSERT COMPLETE")
print("="*60)
