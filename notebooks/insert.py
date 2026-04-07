# ============================================================
# MASTER INSERT — seeds all demo-acme data in dependency order
# Catalog: playground_prod (see KNOWN LIMITATIONS in MASTER_SCRIPTS_PLAN.md)
# Run from Databricks notebook cell:
#   exec(open("/tmp/seed-data/notebooks/insert.py").read())
# ============================================================

import re
from datetime import date, timedelta

BASE = "/tmp/seed-data/notebooks"

# Rolling 1-year window ending today — update narrative.yaml before any sub-script runs
_today = date.today()
_start = _today - timedelta(days=365)
_yaml_path = "/tmp/seed-data/config/stories/narrative.yaml"

with open(_yaml_path) as _f:
    _yaml = _f.read()

_yaml = re.sub(r'^start_date: ".*"', f'start_date: "{_start.isoformat()}"', _yaml, flags=re.MULTILINE)
_yaml = re.sub(r'^end_date: ".*"',   f'end_date: "{_today.isoformat()}"',   _yaml, flags=re.MULTILINE)

with open(_yaml_path, "w") as _f:
    _f.write(_yaml)

print(f"Date window: {_start.isoformat()} → {_today.isoformat()}")

def run(path):
    print(f"\n{'='*60}")
    print(f"  RUNNING: {path}")
    print(f"{'='*60}\n")
    exec(open(f"{BASE}/{path}").read(), globals())

# 1. core — enterprise copilot metrics (enterprise_id=999999)
run("core/insert.py")

# 2. direct — GitHub + Jira data (org=demo-acme-direct)
#    Must run before dora/ (dora CTFC chart depends on mt_itsm_issues_hist)
run("direct/insert.py")

# 3. dora — pipeline events, sdm tables, filter config
#    Depends on direct/'s mt_itsm_issues_hist
run("dora/insert.py")

# 4. release_mgmt — release management (fix_version LIKE 'demo-%')
run("release_mgmt/insert.py")

# 5. snaplogic — SnapLogic integration data (org=demo-acme-direct)
run("snaplogic/insert.py")

print("\n" + "="*60)
print("  MASTER INSERT COMPLETE")
print("="*60)
