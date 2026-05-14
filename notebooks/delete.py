# ============================================================
# MASTER DELETE — removes all demo-acme data in reverse order
# Catalog: playground_prod (see KNOWN LIMITATIONS in MASTER_SCRIPTS_PLAN.md)
# Run from Databricks notebook cell:
#   exec(open("/tmp/seed-data/notebooks/delete.py").read())
# ============================================================

BASE = "/tmp/seed-data/notebooks"

def run(path):
    print(f"\n{'='*60}")
    print(f"  RUNNING: {path}")
    print(f"{'='*60}\n")
    exec(open(f"{BASE}/{path}").read(), globals())

# Reverse of insert order
# 1. code_reliability — Code Reliability dashboard tables (Acme + Meridian)
run("code_reliability/delete.py")

# 2. value_stream — Issue Stream / Flow View fact table (self-contained, no deps)
run("value_stream/delete.py")

# 3. snaplogic
run("snaplogic/delete.py")

# 4. release_mgmt
run("release_mgmt/delete.py")

# 5. ai_compare — AI Code Comparison rows. Runs BEFORE dora/ so its
#    filter_values_unity rows (created_by='seed-data-ai-compare@demo.io')
#    are cleaned up before dora removes the parent filter_group.
run("ai_compare/delete.py")

# 6. meridian — Meridian Analytics rows (DORA, copilot, devex, CRs, releases,
#    filter group, jira board). Cleans its own filter_values + filter_group.
run("meridian/delete.py")

# 7. devex — delete devex filter values before dora deletes the filter group
run("devex/delete.py")

# 8. dora — deletes the filter group itself (after devex values are already gone)
run("dora/delete.py")

# 9. direct
run("direct/delete.py")

# 10. core
run("core/delete.py")

print("\n" + "="*60)
print("  MASTER DELETE COMPLETE")
print("="*60)
