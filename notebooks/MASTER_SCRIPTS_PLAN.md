# Master Insert/Delete Scripts — Implementation Plan

**Status:** Not yet implemented. This document is the working plan for the next session.

---

## Goal

Create two orchestration scripts at the `notebooks/` level:

- `notebooks/insert.py` — runs all 5 sub-notebooks in dependency order
- `notebooks/delete.py` — runs all 5 sub-notebooks in reverse order

These scripts exec() each sub-script rather than re-implementing any logic. Each sub-script already handles its own scoping and error handling.

---

## File Locations

```
notebooks/
  insert.py          <-- NEW: master orchestrator
  delete.py          <-- NEW: master orchestrator
  core/insert.py
  core/delete.py
  direct/insert.py
  direct/delete.py
  dora/insert.py
  dora/delete.py
  release_mgmt/insert.py
  release_mgmt/delete.py
  snaplogic/insert.py
  snaplogic/delete.py
```

The repo is synced to `/tmp/seed-data` on the Databricks cluster. Scripts are invoked from a notebook cell using the pattern:

```python
exec(open("/tmp/seed-data/notebooks/insert.py").read())
```

---

## Dependency Graph

```
core/         (no deps)          ]
direct/       (no deps)          ]  can run in any order relative to each other
release_mgmt/ (no deps)          ]  but all must complete before dora/
snaplogic/    (no deps)          ]

dora/         depends on direct/'s mt_itsm_issues_hist (CTFC chart)
              must run AFTER direct/ on insert
```

### Insert order (must be respected)

1. `core/`
2. `direct/`
3. `dora/`          ← direct/ must be done before this
4. `release_mgmt/`
5. `snaplogic/`

### Delete order (reverse)

1. `snaplogic/`
2. `release_mgmt/`
3. `dora/`
4. `direct/`
5. `core/`

Delete order for dora/direct does not create a referential integrity problem (deletes are independent), but reversing insert order is the safe convention and should be followed.

---

## Scoping Reference (for audit/safety comments in code)

| Sub-script     | Scope key              | Value                  | Catalog          |
|----------------|------------------------|------------------------|------------------|
| core           | enterprise_id          | 999999                 | playground_prod  |
| direct         | org_name / customer_id | demo-acme-direct       | playground_prod  |
| dora (sdm)     | level                  | demo-acme-corp         | playground_prod  |
| dora (pipeline)| record_inserted_by     | seed-data              | playground_prod  |
| dora (filter)  | createdBy / created_by | seed-data@demo.io      | playground_prod  |
| dora (jira)    | board_id               | 1                      | playground_prod  |
| release_mgmt   | fix_version            | LIKE 'demo-%'          | playground_prod  |
| snaplogic      | org / org_label        | demo-acme-direct       | playground_prod  |

---

## Pseudocode

### notebooks/insert.py

```python
# ============================================================
# MASTER INSERT — seeds all demo-acme data in dependency order
# Catalog: playground_prod (see KNOWN LIMITATIONS below)
# Run from Databricks notebook cell:
#   exec(open("/tmp/seed-data/notebooks/insert.py").read())
# ============================================================

BASE = "/tmp/seed-data/notebooks"

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
```

### notebooks/delete.py

```python
# ============================================================
# MASTER DELETE — removes all demo-acme data in reverse order
# Catalog: playground_prod (see KNOWN LIMITATIONS below)
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
```

---

## Known Limitations

### 1. CATALOG is hardcoded in each sub-script

Every sub-script independently declares `CATALOG = "playground_prod"`. The master script cannot override this from a single place at the top. If the catalog ever needs to change (e.g. for a different environment), all 10 sub-scripts must be updated individually.

**Recommended follow-up:** Refactor sub-scripts to read `CATALOG` from a global variable if set, falling back to the hardcoded default:

```python
CATALOG = globals().get("CATALOG", "playground_prod")
```

Then the master script can set `CATALOG = "playground_prod"` once at the top and sub-scripts will inherit it via `globals()`.

### 2. exec() shares globals

The `exec(..., globals())` pattern means variables defined in one sub-script are visible to later sub-scripts. This is generally fine (and required for the CATALOG override approach above), but namespace collisions are possible if sub-scripts define variables with the same names. This has not caused issues to date but is worth noting.

### 3. No partial-run recovery

If `dora/insert.py` fails after `direct/insert.py` has already run, the master script stops. There is no automatic rollback or resume-from-step capability. Recovery requires manually running the remaining sub-scripts or running delete/ then re-running insert/.

---

## Implementation Steps (for next session)

1. Open `/Users/diegovillafuerte/Documents/Github/seed-data/notebooks/` in your editor.

2. Create `notebooks/insert.py` using the pseudocode above verbatim. The `run()` helper and section headers are the only new logic — everything else delegates to the sub-scripts.

3. Create `notebooks/delete.py` using the pseudocode above verbatim.

4. Test on the Databricks cluster by running from a notebook cell:
   ```python
   exec(open("/tmp/seed-data/notebooks/insert.py").read())
   ```
   Confirm all 5 section headers print and no errors are raised.

5. Test delete in the same way. Confirm all rows are removed by spot-checking one table per sub-script.

6. Optional follow-up (separate PR): refactor all 10 sub-scripts to use `globals().get("CATALOG", "playground_prod")` so the master script can set CATALOG once. Update this plan when done.

---

## Safety Checklist

Before running either master script on the cluster, verify:

- [ ] Repo is synced to `/tmp/seed-data` (git pull or re-sync)
- [ ] You are connected to `playground_prod` only — no other catalog should be touched
- [ ] You are scoped to demo-acme-direct / enterprise_id 999999 data only — existing org data must not be affected
- [ ] Run delete before insert if re-seeding, not insert on top of existing data
