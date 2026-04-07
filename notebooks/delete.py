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
# 1. snaplogic
run("snaplogic/delete.py")

# 2. release_mgmt
run("release_mgmt/delete.py")

# 3. dora
run("dora/delete.py")

# 4. direct
run("direct/delete.py")

# 5. core
run("core/delete.py")

print("\n" + "="*60)
print("  MASTER DELETE COMPLETE")
print("="*60)
