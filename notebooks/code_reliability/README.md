# Code Reliability Dashboard — Seed Plan

URL: `/insights/v2/code-reliability/dashboard/overview/c1a181cf-f40f-4ddf-87a2-61045827191f`

This dashboard renders multiple security/quality "domains". The widgets read from
several base tables, only some of which we already seed. This document captures
the gap analysis and the plan to seed what's missing — same narrative arcs as
the rest of our demo data (Acme: incident spike, Meridian: pre/post-Opsera
inflection).

> Status: planning. Diagnostic scripts in this folder confirm schemas before we
> write the generators. **Nothing here writes data yet.**

---

## Source-of-truth: API SQL files

All paths under `vnxt-insights-api-main/src/queries/template-common/`:

| Domain | Folder | Base tables (FROM clauses) |
|---|---|---|
| SonarQube — coverage / complexity / quality gate | `sonarqube-coverage/` | `base_datasets.asp_sonar_measures` |
| SonarQube — Reliability / Security / Maintainability ratings | `sonar-ratings/` | `base_datasets.asp_sonar_measures` + `base_datasets.asp_sonar_issues` |
| SonarQube — defect density | `sonarqube_def_dens/` | `base_datasets.asp_sonar_measures` |
| SonarQube — coverage trend chart | `code_coverage_trend/` | `base_datasets.asp_sonar_measures` |
| GitHub Security — code/secret/dependency scanning | `github-security/` | `base_datasets.code_scan_alert`, `base_datasets.secret_scan_alert`, `base_datasets.dependabot_scan_alert` |
| Twistlock container security | `twistlock-security/` | `base_datasets.twistlock_security_issues` |
| Web App Security (Invicti / WAS) | `web-app-security/` | `source_to_stage.raw_invicti_data`, `source_to_stage.raw_invicti_all_issues` |

Filter wiring for every domain goes through `master_data.v_filter_group_values_kpi_flattened_unity`
(same chain as DORA — `filter_groups_unity` → `filter_values_unity` → flattened view).

---

## Gap analysis — what's already seeded vs missing

| Table | Status | Notes |
|---|---|---|
| `base_datasets.code_scan_alert` | ✅ seeded | `generators/code_scan_alert.py` — already drives the GHAS code scanning series |
| `base_datasets.secret_scan_alert` | ✅ seeded | `generators/secret_scan_alert.py` — already drives the GHAS secret scanning series |
| `base_datasets.asp_sonar_measures` | ❌ missing | project-level metrics: coverage %, ratings, ncloc, sqale_index |
| `base_datasets.asp_sonar_issues` | ❌ missing | issue-level rows: type ∈ {BUG, VULNERABILITY, CODE_SMELL}, severity, status |
| `base_datasets.dependabot_scan_alert` | ❌ missing | mirrors code_scan_alert shape — likely small generator |
| `base_datasets.twistlock_security_issues` | ❌ missing | container CVE scans — has `cve` array, `unique_sha_id` dedup |
| `source_to_stage.raw_invicti_data` | ❌ missing | WAS scan summary rows; one row per scan |
| `source_to_stage.raw_invicti_all_issues` | ❌ missing | per-vuln rows; left-joined to scans |

Existing GHAS generators already encode the Acme story (calm baseline + March-2026
SEV1 spike + smaller Nov-2025 spike). The new generators should layer onto that
narrative, not replace it.

---

## Story arcs

### Acme (steady-state with security incident spike)
- Sonar measures: coverage hovers ~70-80% with a dip during the March incident.
  Ratings stay at A/B for Reliability + Maintainability; Security drops from A → C
  during the spike then recovers.
- Sonar issues: ~10-30 BUG, ~5-15 VULNERABILITY, ~50-150 CODE_SMELL open at any time;
  spike weeks see +50% VULNERABILITY rows.
- Twistlock: 2-3 critical CVEs in the spike window, otherwise mostly low/medium.
- Invicti / WAS: weekly scans; spike window includes one scan with several high-severity findings.
- Dependabot: 1-2 alerts/week baseline, +5 during spike.

### Meridian (pre/post-Opsera inflection at t=0.5)
- Sonar coverage: 40% → 75% across the year (post-Opsera quality gates).
- Sonar issues: ~200 CODE_SMELL pre-Opsera shrinking to ~50 post; vulnerability
  count drops 80% post-inflection.
- Twistlock: pre-Opsera shows accumulating critical CVEs (no automated patching);
  post-Opsera the count drops sharply.
- WAS: scan cadence weekly throughout but findings drop post-Opsera.
- Dependabot: alerts cleared faster post-Opsera (`fixed_at` shorter delta).

---

## Hierarchy / filter mapping (must match other dashboards)

```
Acme:     level_1='Acme Corp',          level_3='demo-acme-corp'    (createdBy='seed-data@demo.io')
Meridian: level_1='Meridian Analytics', level_3='demo-meridian'     (createdBy='seed-data-meridian@demo.io')
```

The Sonar / Twistlock / WAS SQL queries all join `project_name` from the source row
to the EXPLODED `project_name` array of `filter_groups_unity`. We must add `project_name`
entries to each org's filter_group so the rows match. Existing DORA filter rows already
carry github URLs and jira project names, so we'll add Sonar/Twistlock/WAS-specific rows
in the new insert script.

Specific `project_name` array values needed (all driven by the source-table column):
- Sonar: same project_name values used in DORA (e.g. `demo-acme-corp/api`, etc.) —
  must align with what the generator emits for `asp_sonar_measures.project_name`.
- Twistlock: image / service names — uses same `project_name` filter column.
- WAS: `WebsiteName` matches a `project_name` array element.

---

## File layout

```
generators/
  dependabot_scan_alert.py        ✓ done — Acme + Meridian
  asp_sonar_measures.py           TODO
  asp_sonar_issues.py             TODO
  twistlock_security_issues.py    TODO
  invicti_was.py                  TODO — emits raw_invicti_data; raw_invicti_all_issues missing in PG
notebooks/code_reliability/
  README.md                       (this file)
  diag_cr_1.py                    schema + counts + samples for all 6 missing tables
  diag_cr_2.py                    filter_groups_unity wiring check (project_name coverage)
  diag_cr_3_dependabot.py         focused dependabot schema + null-rate inspection
  insert.py                       ✓ unified insert — loops over both orgs, calls each generator
  delete.py                       ✓ unified delete — scoped per table, both orgs
notebooks/insert.py               ✓ wired — run("code_reliability/insert.py") as step 8
notebooks/delete.py               ✓ wired — run("code_reliability/delete.py") as step 1 reverse
```

**Convention:** all Code Reliability work lives in `code_reliability/` and seeds
BOTH demo orgs from a single insert script (value_stream pattern). New tables
get added by appending a generator import + entry to the `GENERATORS` list in
`insert.py`, and a `(table, predicate)` row to `delete.py`.

`code_scan_alert` and `secret_scan_alert` remain in `direct/` + `meridian/`
because they predate this dashboard and feed older charts too — leaving them
in place to avoid churn on working code.

---

## Diagnostic-first workflow

Before writing any generator, run the diag scripts on the cluster to confirm:
1. Each missing table actually exists in `playground_prod` (and which schema).
2. Real columns + types (DESCRIBE output) — needed for the INSERT SQL.
3. Whether any demo rows are already present (would be a surprise — investigate first).
4. Whether the `project_name` values our DORA/value_stream generators emit are
   compatible with the Sonar/Twistlock/WAS join shape.

```python
exec(open("/tmp/seed-data/notebooks/code_reliability/diag_cr_1.py").read())
```

Diag output is JSON/compact dicts (no Spark `.show()` borders) so it can be pasted
back here cleanly.

---

## Scoping (for delete.py — to be defined after diag run)

Tentative scope columns. Confirm during diag — these may not all exist:

| Table | Acme delete predicate | Meridian delete predicate |
|---|---|---|
| `asp_sonar_measures` | `org_name = 'demo-acme-direct'` (TBD) | `org_name = 'demo-meridian'` (TBD) |
| `asp_sonar_issues` | same | same |
| `twistlock_security_issues` | `tool_identifier='twistlock' AND project_name LIKE '%demo-acme%'` (TBD) | `... LIKE '%demo-meridian%'` (TBD) |
| `raw_invicti_data` | `WebsiteName LIKE '%demo-acme%'` (TBD) | `WebsiteName LIKE '%demo-meridian%'` (TBD) |
| `raw_invicti_all_issues` | join on WebsiteId from raw_invicti_data scope | same |
| `dependabot_scan_alert` | `organization = 'demo-acme-direct'` (TBD) | `organization = 'demo-meridian'` (TBD) |

If a table has no clean org-scoping column, fall back to a unique generator-tag
column like `record_inserted_by = 'seed-data'` / `'seed-data-meridian'` (same
trick used in dora/ + value_stream/).

---

## Open questions (resolve in next session)

1. Does the Code Reliability dashboard at UUID `c1a181cf-...` show a fixed widget
   set, or is it user-customizable? If user-customizable we may not need every
   domain — focus on whatever widgets are on the default layout for the demo.
2. Do we already have a demo-meridian filter_groups_unity row that surfaces
   project_name values needed for Sonar SQL joins? (diag_cr_2 answers this.)
3. Are there KPI UUIDs specific to this dashboard (like the CTFC UUIDs) that we
   need to wire `filter_values_unity.kpi_id` rows for? Search
   `vnxt-insights-api-main/src/queries/kpiIdentifierConfig.json` for sonar/twistlock/was/
   dependabot KPI names — listed at top of `diag_cr_2.py`.
