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

---

## 6. Issue Stream / Flow View — copilot_commit_flag inconsistent type across SQL queries

**Charts:** Value Stream → Issue Stream "Top issues by flow references" (`ai_assisted_commits` count)
**Symptom:** `ai_assisted_commits` always reports 0 in the Issue Stream list panel even when commits do have a Copilot flag set; AI Assist tab and Commit metrics tab show correct numbers.
**Root cause:** The `copilot_commit_flag` column is referenced inconsistently across `vnxt-insights-api/src/queries/value-stream/`:
  - `issue-stream-list.sql:20` uses `copilot_commit_flag = true` (boolean literal)
  - `flow-dashboard-commit-metrics.sql:7,19,20` and `flow-dashboard-aiassist.sql:39` use `copilot_commit_flag = 'Y'` (string literal)

The seed-data generator stores `'Y'` to satisfy the 2-of-3 majority of queries, so the issue-stream-list comparison `'Y' = true` casts to `'Y' = 'true'` which is false — yielding 0.
**Fix:** Standardize all SQL files to use the same convention. Recommended: change `issue-stream-list.sql:20` to `copilot_commit_flag = 'Y'` to match the other two files.

---

## 7. Issue Stream detail view 500s on first request — ML service async response not handled

**Chart:** Value Stream → Issue Stream → click any issue (e.g. `/insights/value-stream/issue/ACME-1001`)
**Symptom:** First request returns 500 with body `{"message":"Summary generation request in progress"}`. Sometimes a second request succeeds.
**Root cause:** `ValueStreamController.getIssueStreamData` calls `getSummaryFromMLService` (`value-stream.controller.js:1029`) for an AI summary. The ML service returns an async-style response on first call (status ≠ 200, body indicates the summary job is in progress). At `value-stream.controller.js:274` the helper throws `Error('ML Service error: ...')` for any non-200 response, which bubbles to the outer catch and returns 500 — there's no polling or progressive-result handling.
**Fix:** In `getSummaryFromMLService`, treat the "in progress" response as a poll-and-wait state (e.g., return cached summary if available, otherwise retry up to N times with backoff before throwing). Alternatively, return the raw issue data on first call without blocking on the ML summary — render the AI summary section client-side once it's ready.

**Note:** Seed data for this feature is correct and complete (`playground_prod.user_working.offerings_jira_pipeline_details` populated for `demo-acme-direct` and `demo-meridian`; the list panel + the underlying SQL both work). This bug is purely API-side.

---

## 8. Flow Dashboard pipeline tabs leak rows across orgs/filters — STAGE_FILTERS missing parentheses

**Charts:** Value Stream → Flow Dashboard → any pipeline-stage tab (Code Review, CI Pipeline, Security, Quality, QA, Deploy, Production)
**Symptom:** With SBG filter set to `Acme Corp` (or any other scoping filter), the pipeline-rows table still shows tickets from every other SBG/org. E.g. on the codeReview tab filtered to `Acme Corp`, MDP-* rows from `demo-meridian` show alongside ACME-* rows.
**Root cause:** `STAGE_FILTERS` constants in `value-stream.controller.js:16-24` are emitted without parentheses around their OR groups:

```js
codeReview: "AND LOWER(pipeline_step_type) LIKE '%review%' OR LOWER(pipeline_step_type) LIKE '%code review%' OR LOWER(pipeline_step_name) LIKE '%review%'"
```

When concatenated into `flow-dashboard-pipeline.sql` alongside the user-filter `whereClause` (which uses `AND`), SQL operator precedence (AND binds tighter than OR) makes the middle OR branches unconstrained:

```sql
(pipeline_id IS NOT NULL AND pipeline_step_name IS NOT NULL AND step_type LIKE '%review%')
 OR step_type LIKE '%code review%'
 OR (step_name LIKE '%review%' AND sbg = 'Acme Corp')
```

So any row matching the bare LIKE (e.g. step_type='review') passes regardless of sbg/org/etc.
**Fix:** Wrap each STAGE_FILTERS entry in parentheses. E.g.: `"AND (LOWER(pipeline_step_type) LIKE '%review%' OR LOWER(pipeline_step_type) LIKE '%code review%' OR LOWER(pipeline_step_name) LIKE '%review%')"`. Apply the same treatment to all 7 entries (codeReview, ci, security, quality, qa, deploy, production).

**Note:** Seed data is correctly tagged — Acme rows have `sbg='Acme Corp'`, Meridian rows have `sbg='Meridian Analytics'`. This is purely an API-side SQL bug.

---

## 9. Jira Issues Analysis shows 0 despite seeded data

**Chart:** Executive Summary (Page 2) → "Jira Issues Analysis" bullet
**Symptom:** "0 issues resolved with Copilot assistance out of 0 total resolved issues" even though `transform_stage.mt_itsm_issues_current` has 266 rows for `customer_id = 'demo-acme-direct'` and `base_datasets.v_itsm_issues_current` surfaces them correctly.
**Root cause:** The backend appears to gate on an active Jira integration being configured in Opsera MongoDB for the org before it queries Databricks ITSM data. Playground/demo instances have no real Jira integration, so the backend short-circuits and returns 0. Attempted inserting a row into `master_data.data_mapping` (Databricks mirror of MongoDB config) with the demo user's ID — ineffective if the backend reads MongoDB directly rather than this table.
**Fix:** Either (a) have the data team configure a mock Jira integration in Opsera MongoDB for the `demo-acme-direct` org, or (b) modify the backend to fall back to `customer_id`-based ITSM queries when no integration is configured (similar to the SCM filter issue in Bug #3).

---

## 10. SPACE dashboard crashes — `space_dimension_metrics.sql` has two SQL bugs

**Chart:** SPACE Developer Experience dashboard (all panels — entire dashboard fails to render)
**Symptom:** `Cannot read properties of undefined (reading 'toFixed')` JS error on render. Fires on every date range we tried (7d, 30d, 90d).
**Root cause:** Two bugs in `vnxt-insights-api/src/queries/template-common/space/space_dimension_metrics.sql`:

1. **`previous_period` is unreachable** (lines 36–40). The `WHERE` clause admits only rows in the current period (`BETWEEN start_date AND end_date`), but the `CASE` above it has a `WHEN ... 'previous_period'` branch keyed on `start_date1`/`end_date1`. No row in the previous-period date range can survive the `WHERE`, so the `previous_period` branch is dead code. The query always returns one row (current_period only). The frontend's period-over-period diff math then reads `previous.s_score.toFixed(...)` on `undefined`. Compare to `space_overview.sql` line 40 which uses `BETWEEN start_date1 AND end_date` (correctly).

2. **Dimension scores inflate with multiple surveys per period** (lines 56–64). Formula is `(dimension_sum * 100.0) / (question_count * respondent_count * 100)`. `dimension_sum` scales linearly with the number of surveys in the window, but `respondent_count = COUNT(DISTINCT response_id)` doesn't (caps at the number of distinct respondents). With weekly surveys + 30-day window we see s_score=1217.50 instead of ~75. The SQL implicitly assumes one survey per dashboard period.

**Fix:** Backend changes — there is no data workaround. Fix #1: change the `WHERE` in `survey_responses` to `BETWEEN start_date1 AND end_date` so previous-period rows can pass. Fix #2: change the divisor to `COUNT(*)` (or `COUNT(DISTINCT survey_id, response_id) * question_count`) so it scales with total responses, not just unique respondents. Optional belt-and-braces: frontend should null-check before `.toFixed()`.

**Note:** Tried switching SPACE survey cadence from monthly to weekly (commit b5cfe20) to defuse the crash — didn't help because the bugs are structural, not data-driven. Reverted-equivalent cadence remains weekly since it doesn't hurt other charts.

---

## 11. Developer Language And Editor Usage dashboard — `all_names` CTE leaks cross-tenant data

**Chart:** Developer Language And Editor Usage (Programming Languages / Editors / IDE Code Completion Models / Chat Models tabs)
**Symptom:** Dropdowns include parameter values from other tenants in the same Databricks catalog. In playground we saw real Opsera-prod custom Copilot finetune deployment names (`copilot-prod-finetune-centralus.opsera-csg-7624or389r9c`, etc.) appearing in the demo-acme-direct dashboard, all reporting 0%.
**Root cause:** `vnxt-insights-api/src/queries/template-common/copilot-reports/copilot_developer_usage.sql:10-13`:
```sql
all_names AS (
   select distinct parameter AS name from {{tableName}}
   where param_name = (SELECT param FROM variables)
)
```
No org filter and no date filter. The customer's `{{secondaryFilterClause}}` is applied to the `source` CTE that produces the metric values, but the catalog of dropdown names is pulled from the entire table across every tenant and every historical date.

**Customer impact (not just a seeding artifact):**
  - **Multi-tenant catalogs:** cross-tenant metadata leak. Customer A's custom Copilot finetune model names appear in Customer B's dashboard dropdown. Numeric values are 0 (correctly scoped), but the names themselves leak across the tenant boundary. Compliance/privacy concern depending on contracts.
  - **Single-tenant catalogs:** historical pollution within the tenant. Any language/editor/model ever recorded stays in the dropdown forever — deprecated tools, former-employee usage, one-off `.dotenv` opens, etc.

**Fix:** Apply `{{secondaryFilterClause}}` to `all_names` as well (it already exists for `source`):
```sql
all_names AS (
   select distinct parameter AS name from {{tableName}}
   where param_name = (SELECT param FROM variables)
   {{secondaryFilterClause}}    -- ← add this
)
```
Optionally also add the date range filter to limit historical pollution for single-tenant customers.

**Note:** Seed data side is now correct — `notebooks/direct/insert.py` populates `base_datasets.github_copilot_developer_usage_org_level` via `generators/copilot_developer_usage.py` (commit 9714f90). Our org's languages, editors, and models surface with real metrics. The cross-tenant leak in the dropdown is a backend bug that affects real customers too, not a demo-data issue.
