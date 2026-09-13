# VisualForge — Seeding Insights

Notes from examining `/Users/diegovillafuerte/Documents/Github/visualforge` (branch `main`, HEAD `b8377ed22`)
on 2026-09-13, from the point of view of **what a future data-seeding process has to produce**.

Everything below is about the *new* front end. Our existing generators target
vnxt-insights-api's SQL-template dashboards; VisualForge is a different consumer of the
same Databricks catalog, so the seed data overlaps but does **not** line up.

---

## 1. The headline: VisualForge is a two-layer consumer, not a SQL-template dashboard

vnxt-insights-api = `SQL template → Databricks → chart`. One hop, our seeded rows show up directly.

VisualForge = **`Databricks → ETL (Node) → MongoDB snapshot → REST → React`**.

- ETL lives in `services/unified-backend/src/modules/integration/databricks/etl/` (~50 `sync*.js` files).
- It writes ~60 Mongo collections, all prefixed `vf_` — `vf_ai_assistants`, `vf_compute_velocity`,
  `vf_dora`, `vf_ghas`, `vf_dvi`, `vf_devex`, `vf_sprintVelocity`, `vf_leadership`,
  `vf_persona_*`, `vf_ai_comparison_*`, `vf_tokenomics_*`. See
  `etl/mongoConceptViewRepo.js` `COLLECTIONS` for the full map.
- Dashboards read Mongo, **not** Databricks, on the hot path.

**Consequence for seeding:** inserting rows into `playground_prod` is only step 1. Nothing appears
until an ETL sync runs and materializes a snapshot. Our seeding runbook needs a step 2.

### Triggering step 2

| How | Detail |
|-----|--------|
| REST | `POST /api/v1/databricks/etl/sync-all`, or per-domain: `/etl/sync/ai-assistants`, `/compute-velocity`, `/dora`, `/ghas`, `/dvi`, `/devex`, `/sprint-velocity`, `/aqp`, `/leadership`, `/code-reliability`, `/persona-metrics`, `/ai-comparison`, `/ai-billing`, `/ai-impact-daily`, `/tokenomics-*`, `/tools/:tool` (see `databricksRouter.js:2605-3250`) |
| Scheduler | `app.js` runs `syncAllConceptViews()` at `ETL_SYNC_HOURS` (default `7,19` UTC) — **only when `NODE_ENV=production|test`**. In dev nothing runs automatically. `DATABRICKS_SYNC_INTERVAL_MS=0` disables. |
| UI | Admin Space → ETL Control, and Mapping Studio has "trigger sync / warehouse poll" buttons |

There's a process-wide ETL lock (`etlConcurrency.js`, `etlRunGuard.js`) — syncs serialize, so a
full reseed + sync-all is a minutes-long operation, not instant.

### ETL lookback windows are per-domain and truncate our history

`syncAll.js` defaults (months): `ai_assistants: 3`, `compute_velocity: 12`, `dvi: 12`,
`code_reliability: 12`, `devex: 3`, `sprintVelocity: 6`, `aqp: 6`, `dora: 12`, `ghas: 12`,
`persona_metrics: 12`, `ai_comparison: 3`. Overridable via `monthsByDomain` in the POST body.
Seeding 12 months of AI-assistant history is wasted unless the caller widens the window.

---

## 2. Catalog resolution is per-user, not per-env

`queryHelpers.js` / `databricksConfig.js`: the catalog prefix comes from the **Opsera scope handler**
for the logged-in user (`@opsera/node-databricks-core` → `DATABRICKS_SCOPE_IDENTIFIERS.INSIGHTS`),
injected as a backtick-quoted prefix into every query (`` `playground_prod`. ``).

To pin a local/demo instance to our seeded catalog:

```bash
DATABRICKS_FORCE_ENV_SCOPE=1
DATABRICKS_FORCE_CATALOG=playground_prod   # or DATABRICKS_CATALOG
VF_LOCAL_FORCE_USER_ID=0                   # else per-user scope wins for Mapping Studio
```

`VF_LOCAL_FORCE_USER_ID=1` silently overrides `DATABRICKS_CATALOG` on the mapping-filter path —
documented as a real footgun in `services/unified-backend/TENANT_SWITCH_RUNBOOK.md`
(they lost an afternoon to Honeywell data showing up after switching to `opsera_test`).

Schema layout is **identical** to vnxt: `source_to_stage` / `base_datasets` / `consumption_layer` /
`master_data` / `user_working` (+ a little `transform_stage` for GitLab Duo). So our
`playground_prod` writes are reusable — the question is only *which tables*.

---

## 3. Table-coverage gap (the actionable part)

Extracted every `FROM`/`JOIN` in `modules/integration/databricks/` (120 distinct tables) and diffed
against what our generators write (~95 tables). Note that roughly a third of VisualForge's table
references omit the `${p}` catalog-prefix variable and resolve against the session's `initialCatalog`
instead — any extraction that only matches prefixed references undercounts.

### 3a. Only 36 tables overlap

`asp_sonar_issues`, `asp_sonar_measures`, `cfr_mttr_metric_data`, `code_scan_alert`,
`commits_rest_api`, `enterprise_user_language_model_level_copilot_metrics`,
`github_copilot_developer_usage_org_level`, `pipeline_activities`, `pull_requests`,
`secret_scan_alert`, `trf_github_copilot_direct_data`, `twistlock_security_issues`,
`v_github_copilot_seats_billing`, `v_github_copilot_seats_usage_user_level`,
`v_github_teams_members_current`, `v_itsm_issues_current`, `v_itsm_issues_hist`,
all six `consumption_layer.ai_assistant_*` / `ai_code_assistant_usage_user_level`, `commits_prs`,
`master_data.{file_extensions, filter_groups_unity, filter_values_unity,
github_copilot_orgs_mapping, v_filter_group_values_kpi_flattened_unity}`,
`junit_test_suite_report`, `raw_github_copilot_billing`, `raw_github_teams_members`,
`raw_invicti_data`, `raw_mongo_transformed_data_gitscraper`,
`raw_sonar_metric_split_data_branchwise`, `raw_sonar_type_data_branchwise`.

Our Copilot + AI-assistant + Sonar/GHAS work carries over best. Everything else needs work.

### 3b. VisualForge reads ~80 tables we don't generate

Clusters worth planning for:

| Cluster | Tables | Why it matters |
|---|---|---|
| **Raw SCM/Jira** | `source_to_stage.raw_github_commits_rest_api`, `raw_github_pull_requests_rest_api_prs(+_details)`, `raw_jira_issues_rest_api`, `raw_jira_issues_changelog_rest_api(+_ci)`, `raw_jira_filter_issues_ci`, `raw_jira_users` | VF computes DORA/DevEx/persona metrics from **raw** tables, not from pre-aggregated ones |
| **CI/pipelines** | `source_to_stage.github_actions_runs_rest_api`, `github_action_jobs_rest_api`, `raw_github_actions_pipeline_activities`, `raw_azp_build_pipelines`, `raw_mongo_pipelineactivities`, `source_to_stage.pipeline_activities` | DVI is built almost entirely from GitHub Actions runs/jobs |
| **Sprints** | `source_to_stage.jira_sprint_rest_api` | Sprint Velocity's only source (we generate `base_datasets.sprint_data` instead) |
| **Date spine** | `master_data.date_dim`, `master_data.v_date_dim` | See §4 — hard blocker |
| **Hierarchy** | `master_data.v_filter_users_values_flattened_unity`, `team_member_list`, `v_workday_emp_hierarchy` | See §5 — hard blocker |
| **Leadership** | `consumption_layer.leadership_time_period_detail` | See §6 |
| **Non-Copilot AI tools** | Cursor (`raw_cursor_usage_data`, `raw_cursor_analytics_dau`, `raw_cursor_user_billing_usage`, `cursor_model_usage`, `raw_cursor_ai_code_commits`, `v_cursor_usage_user_level`, `cursor_team_analytics`, `cursor_team_acceptance_info`), Claude (`raw_claude_org_users`, `raw_claude_code_usage_report`, `raw_claude_usage_report_messages`, `claude_code_usage_report_agg`), Windsurf (`windsurf_group_member_info`, `raw_windsurf_user_cascade_analytics`, `windsurf_cascade_analytics`, `rawdatawindsurf_cascade_lines`, `raw_windsurf_user_page_analytics`), Gemini (`raw_gemini_usage_data`, `raw_gemini_member_data`, `v_gemini_user_level_info`), Factory AI (`factory_ai_*` ×5), Adobe (`adobe_ai_user_usage_daily(+_models)`), GitLab Duo (`transform_stage.gitlab_duo_ai_*`) | Our AI Code Comparison batch seeds the consumption-layer views; VF wants the **raw per-tool** tables |
| **Bitbucket** | `raw_bitbucket_onprem_{users,repos,commits_rest_api,pull_requests_*}` | Whole alternate SCM path we've never seeded |
| **New-suffix Copilot** | `github_copilot_developer_usage_org_level_new`, `..._teams_level_new`, `github_copilot_metrics_ide_org_level_new` | See §7 |
| **Misc** | `v_ghas_overview`, `copilot_time_to_pr_cycle_time_ltfc`, `github_copilot_premium_request_usage`, `org_user_level_copilot_metrics`, `pr_survey_time_savings_enriched`, `v_itsm_issues_current_aqp`, `v_itsm_issues_hist_pr`, `v_sonar_issues_current`, `asp_security_issues`, `fix_version`, `aop_census_mock_data` | one-off per-dashboard deps |

### 3c. ~60 tables we generate that VisualForge never reads

The entire `consumption_layer.sdm_*` family (`sdm_df`, `sdm_ltfc`, `sdm_cfr`, `sdm_mttr`, their
`_wkly` variants, `sdm_daily_snapshot`, `sdm_weekly_snapshot`), plus `dora_metrics`,
`dora_summary`, `deployment_frequency`, `lead_time_for_changes`, `change_failure_rate`,
`mean_time_to_recovery`, `release_management_detail(_v2)`, `pr_metric_view`,
`base_datasets.{deployments, incidents, jira_boards, sprint_data, pipeline_deployment_commits,
dependabot_scan_alert}`, all `master_data.*_table` / `velocity_dashboards` /
`gitcustodian_*` catalog tables, all Snaplogic tables, `raw_sonar_project_branch_list`, the SPACE survey
tables, and our `user_working` Value-Stream tables (`offerings_jira_pipeline_details`,
`repo_pipeline_details`, `github_offering_workflow_job_logs`).

**This is the biggest strategic point:** our DORA/release-management/SPACE/Value-Stream/Snaplogic
generators produce data VisualForge structurally cannot see. VF re-derives DORA from raw PRs,
commits and ITSM issues. Either (a) we add raw-table generators and let VF compute, or
(b) VF grows readers for the `sdm_*` layer. Worth deciding before writing more generators.

*(Caveat: `master_data.*` catalog tables and `gitcustodian_*` are vnxt dashboard-registry tables —
VF has its own KPI catalog in Mongo, so those legitimately don't apply.)*

---

## 4. `master_data.date_dim` is a hard blocker for every time series

VF builds trend charts by **`LEFT JOIN` onto the date spine**, then aggregating — e.g.
`aiAssistantsQueries.js:777,861,901,1318,1356`, `leadershipQueries.js:46`,
`sprintVelocityQueries` (`v_date_dim`).

Columns consumed: `calendar_date`, `week_start_date`, `week_end_date`, `month_start_date`,
`month_end_date`.

If `date_dim` doesn't cover the seeded window, **charts render empty even though the fact rows
exist** — and the failure is silent (no error, just zero buckets). Our generators never touch
`date_dim`; we've been relying on whatever pre-existed in `playground_prod`. Before a VisualForge
seed: verify coverage of the full seed window, including the trailing month, and note that
`leadershipQueries.periodVariables` *shifts the window back one month* when `to_date` is the
current month.

---

## 5. Hierarchy + user mappings gate every dashboard

Two coupled gates:

1. **Databricks side** — `master_data.filter_groups_unity` and
   `master_data.v_filter_users_values_flattened_unity` supply `level_1..level_5` and (for the users
   view) a per-row `usage_date`. `filterUsersUnityAllQuery` filters
   `usage_date BETWEEN from AND to`, so a user with no row in the window is invisible.
   `master_data.team_member_list` (Workday path) uses `LATERAL VIEW EXPLODE(reportees)` and appends
   a tenant suffix (`_cisco`, `_HON`) to build `assignee_login` — synthetic logins must match that
   convention if the Workday path is on.
2. **Mongo side** — `vf_mappingGroups` + `vf_user_mappings` (Mapping Studio). ETL turns these into
   `vf_hierarchy_filters` (`etl/filterUsersFromUserMappings.js`).

Two specific traps:

- **`level_1` is mandatory.** `isInvalidLevel1` drops rows with blank/null `level_1`.
  Placeholders (`—`, `-`, `N/A`) are kept; blanks are dropped. `level_5` is stricter — `0`, `—`,
  `-`, `N/A` are all treated as invalid.
- **Date-range intersection.** `computeVelocityUserMappingScope.js`: a developer's activity counts
  only on days in `dashboard [start,end] ∩ user mapping date_ranges`. Seed a user whose mapping
  range doesn't span the activity and their metrics are **zero, not missing** — looks like a data
  bug, is actually a mapping bug.

So a VisualForge seed is not just Databricks rows: it needs matching mapping groups + user mappings
with correct level values and date ranges, either seeded into Mongo directly (see
`scripts/dev/seed-mappings-and-data.mjs`, tenant `local-demo`, DB `visualforge`) or created via
Mapping Studio.

---

## 6. Leadership dashboard needs a VARIANT table keyed by KPI UUID

`consumption_layer.leadership_time_period_detail` — a single wide table read with
`variant_explode_outer(result)` and filtered by hardcoded `kpi_uuids`
(`leadershipQueries.js:6-20`):

| KPI | UUID |
|---|---|
| Deployment Frequency | `60aed2f8-1c74-4792-ad51-bf4e5a65f7b9` |
| LTFC | `a9337c02-a00e-40ad-9cdc-2d18dfd771c9` |
| Time to PR | `2f5f0c7e-3fd7-41b4-883a-18d3da5b7fa3` |
| CFR | `ab9a59ba-a19c-4358-b195-1648797f77c2` |
| MTTR | `906f4f2b-a299-4b24-9a24-2330f45dd493` |
| SCAS | `c3b1d45e-8a72-4c9f-b012-5678abcdef01` |
| Say-Do | `f60d8a58-7c8d-4dd6-9b54-6c07715ae5ec` |
| Defect Density / PR Size | `9fd5ec78-9fce-49a0-8154-24d3109d3f05` |
| AI Acceptance / AI LOC | `5d3c39cb-de4b-41b2-b26f-140d1bb1ebfc` |
| AI Adoption | `b39ae2aa-2cb7-425a-b2e4-167cbe8a890a` |

Rows carry a nested `result` VARIANT with per-KPI fields (`pipeline_source`, `pipeline_id`,
`step_id`, `pipeline_run_count`, `step_status`, `is_pipeline_success`, `is_pipeline_failure`,
`commit_size`, `is_defect`, …). Note Defect Density and PR Size share a UUID, and so do AI
Acceptance and AI LOC — generator has to distinguish them by payload fields, not UUID.

This is a genuinely different generator shape from anything we have: one table, VARIANT payload,
UUID-discriminated.

---

## 7. `_new` table suffixes — our Copilot generators point at the deprecated names

VF deliberately reads the `_new` variants and says so in comments
(`aiAssistantsQueries.js:70-75`, `copilotRawQueries.js:156-157`):

- `base_datasets.github_copilot_metrics_ide_org_level_new` — *"the old
  `v_github_copilot_metrics_ide_org_level` view is deprecated and may have no data"*
- `base_datasets.github_copilot_developer_usage_teams_level_new` (hierarchy mode, joined to
  `team_member_list`) and `..._org_level_new` (basic mode)

We generate `github_copilot_metrics_ide_org_level` and `github_copilot_developer_usage_org_level`
(no suffix). One nuance: `aiAssistantsQueries.js:376` explicitly prefers the **un-suffixed**
`github_copilot_developer_usage_org_level` for one query, *"NOT
`..._org_level_new` which may have different/no data."* So both variants are live for different
queries — we likely need to write both, with the same underlying numbers.

---

## 8. Tenant-name traps

### 8a. Anything with "playground" in the tenant id gets hardcoded fixtures

`services/playgroundCopilotUserNames.js`:

```js
export function isPlaygroundTenant(tenantId) {
  const key = String(tenantId ?? '').trim().toLowerCase().replace(/[^a-z0-9]/g, '');
  return key.includes('playground');
}
```

When true:

- `playgroundAiAssistantsFixture.js` **overrides the AI Code Assistants response** with synthetic
  Cursor/Copilot/Windsurf data derived from a hardcoded roster of *real Opsera logins*
  (`pkariappa`, `himatej-auku`, `pradeepmarri`, … with activeDays and dates through 2026-06-30) and
  fixed multipliers (`PLAYGROUND_TOOL_USAGE`: Cursor 1.22 > Copilot 1.0 > Windsurf 0.78).
- `aiToolTenancyAvailability.js:234` force-enables all four tool tabs.
- `cursorInteractionFixture.js` supplies synthetic Cursor interaction scores.

**Our seeded data would be masked by these fixtures**, and the demo would show Opsera employee
logins instead of our synthetic roster. Note the check is on the **tenant id**, not the catalog —
so `playground_prod` as a catalog is fine, but a tenant named `*playground*` is not. If a demo
tenant has to be called that, the fixture path needs a bypass.

### 8b. `pipeline_activities.customer_id` is a Mongo ObjectId

From `UAT_DATABRICKS_RUNBOOK.md` + `cisoPipelineQueries.js:14` (`AND customer_id = '...'`):
CISO pipeline-gate queries filter on `customer_id`, resolved via
`DATABRICKS_CUSTOMER_ID_MAP='{"opsera.us":"68f0d176f5c70d2b7a3f4aae"}'` (or
`DATABRICKS_CUSTOMER_ID`). It is **not** the tenant domain. Seeded `pipeline_activities` rows need
a `customer_id` that matches whatever the map resolves to, or CISO gate metrics go blank.
(`consumption_layer.commits_prs` has no `customer_id` — catalog + `SELECT` is enough there.)

### 8c. Mongo docs whose ids start with `seed:` are dropped on read

`financialMetricsFromMongo.js` drops `seed:*` and `cfo_sample` docs from CFO dashboard reads. If we
ever seed Mongo directly, **do not prefix ids with `seed:`** — the dashboard filters them out
deliberately.

---

## 9. Serve-time fallbacks make "is the data flowing?" ambiguous

`UAT_DATABRICKS_RUNBOOK.md` documents a fallback chain per persona: when Mongo is sparse the
backend silently fills from Databricks, and when that's empty it derives from other Mongo
collections or a cost model with env-var constants (`CFO_INCIDENT_COST_PER_HOUR=5000`,
`CFO_AUTOMATION_SAVINGS_PER_DEPLOY=75`, …). Examples: CISO `security_vuln_by_severity` from
`asp_sonar_issues`; Leadership `merged_prs` from `commits_prs`; CTO `ai_code_coverage` from
`vf_persona_commits` → `commits_prs`.

Fallbacks apply only when the Mongo metric is **missing or zero** — so a chart can look correct
while reading from a completely different source than we seeded. Validating a VisualForge seed
means checking the source, not just the number. Their own `scripts/dev/validate-*-kpis.mjs`
(ciso / cfo / cto / vp / cpo / leadership / tokenomics-parity / databricks-user-scope) are the
right tools and worth reusing.

Also: `sprintVelocityQueries.js` and `personaQueries.js` probe `information_schema.tables` /
`information_schema.columns` at runtime to pick table variants (e.g. `raw_jira_boards` vs
`raw_jira_boards_ci`). Table *existence* changes behavior, so creating an empty table is not
neutral.

---

## 10. Feature flags and snapshot contracts

DORA and GHAS read the Databricks snapshot **only when a feature flag is on**; otherwise they use
the legacy WorkItems / security API path (`etl/DORA_GHAS_ETL_CONTRACT.md`). That doc also specifies
the exact snapshot shape ETL must write — `vf_dora` needs `commits[]`, `prs[]` (with open/merge/close
timestamps for LTFC + CFR), `jiraIssues[]` (bug lifecycle timestamps for MTTR) and a
`stability: { cfrPct, mttrHours, incidents, weeklySeries }` override; `vf_ghas` needs ~18 fields.
Useful as the acceptance spec for a DORA/GHAS seed.

---

## 11. Things that make seeding *easier*

- **UI-only mode**: `cd frontend && npm run dev:ui` runs with rich fixture stubs — no Mongo,
  no Kong, no Databricks. Good for front-end-only demos; irrelevant if the point is real data.
- **Their demo seed path**: `npm run demo:integration-up` + `npm run demo:seed` seeds Mongo
  directly for tenant `local-demo`. `scripts/dev/seed-mappings-and-data.mjs` creates 4 mapping
  groups with distinct `level_1` scopes + work items + `vf_metrics` + sprints + security findings.
  A reasonable reference shape for the Mongo half of a seed. (Note `package.json`'s `demo:seed`
  points at `scripts/dev/seed-demo-dashboard-data.mjs`, which **doesn't exist** in the repo — that
  script is stale.)
- **~45 validation/diagnostic scripts** in `scripts/dev/` — `run-all-databricks-etl.mjs`,
  `list-source-to-stage-tables.mjs`, `databricks-connection-dry-run.mjs`,
  `validate-databricks-user-scope.mjs`, `verify-hierarchy-rollup.mjs`, the `validate-*-kpis.mjs`
  family. These are the post-seed verification harness; we shouldn't rebuild them.
- **`record_insert_datetime` convention carries over** — heavily filtered across 14 query files
  (20 uses in `aiAssistantsQueries.js` alone), same as vnxt. Our existing freshness handling
  applies, including the `v_github_teams_members_current` global-MAX trap in `BUGS.md` §2, which
  bites VisualForge identically since it reads that same view.

---

## 12. Open questions to settle before building generators

1. **Raw vs aggregated DORA** — do we generate `source_to_stage.raw_github_*` / `raw_jira_*` and let
   VF compute DORA, or ask VF to read our `sdm_*` layer? (§3c)
2. **Which tenant id** will the demo use, and does it contain "playground"? (§8a)
3. **Is `playground_prod.master_data.date_dim` populated through the seed window** — including the
   trailing/current month? (§4)
4. **Who runs the ETL after seeding**, and with what `monthsByDomain`? A seed handoff that doesn't
   include a sync-all is a seed that shows nothing. (§1)
5. **Scope** — 120 tables is not a sprint. Suggested slice: AI Code Assistants + Tokenomics first
   (largest overlap with existing generators), then DVI/Sprint (new CI + sprint raws), then
   Leadership (new VARIANT shape) last.
