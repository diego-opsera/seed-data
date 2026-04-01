# Post-Demo Jira Tickets

Issues discovered during demo seeding on 2026-04-01.
All affect the demo-acme-direct playground instance.

---

## 1. copilot_license_overview.sql — wrong filter template variable

**Chart:** Executive Summary → "X Copilot Licenses"
**Symptom:** Shows 0 licenses despite 150 allocated seats in the data.
**Root cause:** `copilot_license_overview.sql` uses `{{additionalFilterClause}}` which resolves to `AND organization = 'demo-acme-direct'`. The view `base_datasets.v_github_copilot_seats_usage_user_level` only exposes `org_name`, not `organization`, so the filter matches nothing.
**Fix:** Change `{{additionalFilterClause}}` to `{{additionalFilterClause_org_name}}` in `copilot_license_overview.sql` (one line).

---

## 2. v_github_teams_members_current — global MAX timestamp pattern blocks new org onboarding

**Chart:** Copilot Usage Metrics → Heavy Users Percentage
**Symptom:** Any new org seeded into `source_to_stage.raw_github_teams_members` with a newer `record_update_datetime` than existing rows will cause all other orgs to disappear from the view.
**Root cause:** The view filters `WHERE record_update_datetime IN (SELECT MAX(record_update_datetime) FROM ...)` — a global max across all orgs, not per-org. New orgs can only be added safely by matching the exact existing max timestamp.
**Fix:** Change the view filter to use a per-org max: `WHERE (org_name, record_update_datetime) IN (SELECT org_name, MAX(record_update_datetime) FROM source_to_stage.raw_github_teams_members GROUP BY org_name)`.

---

## 3. Activity charts — org isolation relies solely on SCM integration filter

**Charts:** Copilot vs Non-Copilot (commits), Open PRs, Merged PRs, Total Commits
**Symptom:** When no GitHub SCM integration is configured (playground/demo mode), `{{additionalSCMFilterClause}}` is empty and all orgs' data bleeds into the selected org's view. Numbers are 10–100x too large.
**Root cause:** The commit/PR activity queries only filter by org via `{{additionalSCMFilterClause}}`, which is only populated when a live GitHub integration exists. Playground instances have no integration.
**Fix:** Apply `{{additionalFilterClause_org_name}}` as a fallback when `{{additionalSCMFilterClause}}` is empty, or add an explicit org filter to the queries that always applies based on the selected org.

---

## 4. MongoDB query cache serves stale results after data re-seed

**Charts:** All Copilot Usage Metrics cards
**Symptom:** After deleting and re-inserting seed data, charts continue showing old (often 0) values until the user manually changes the date range.
**Root cause:** Query results are cached in MongoDB keyed by a hash of request parameters. Re-seeding the underlying Databricks tables does not invalidate the cache.
**Fix:** Add a cache-busting mechanism for playground/demo accounts (e.g. a "refresh data" button, TTL reduction, or admin endpoint to clear cache by org).

---

## 5. PR velocity and hours saved require consumption layer ETL

**Charts:** Executive Summary → PR velocity, hours saved/month, monthly savings
**Symptom:** All three show 0/N/A. `consumption_layer.commits_prs` and `consumption_layer.ai_assistant_acceptance_info` have 0 rows for demo-acme-direct.
**Root cause:** These tables are ETL-derived from base tables. The consumption layer pipeline has not been run against the seeded base data.
**Fix:** Either run the consumption layer pipeline against playground_prod for demo-acme-direct, or document which base tables must be seeded to allow direct insertion into the consumption layer tables (bypassing ETL).
**Note:** `ai_assistant_acceptance_info` and `ai_code_assistant_usage_user_level` were successfully seeded directly (generators exist). `commits_prs` still needs ETL or a direct-seed generator.
