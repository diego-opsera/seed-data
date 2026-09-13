# VisualForge DVI — Phase 0 Findings

Run date **2026-09-13** against `playground_prod`, via
`vf/notebooks/dvi/{diag_catalog,diag_sources,diag_dimensions}.py`.

**Headline: the DVI dashboard is not starved of data.** All five dimensions already resolve
catalog-wide, and the predicted snapshot score is **76.2 / 100** — not the `N/A` / `0` the UI shows.
That moves the problem from *seeding* to *scoping, staleness and saturation*, and materially revises
[`dvi_seeding_plan.md`](dvi_seeding_plan.md).

---

## 1. Predicted tiles vs. what the UI shows

| Dimension | Raw (catalog-wide) | Predicted score | UI shows |
|---|---|---|---|
| Velocity | 0.32 median cycle hours | **100.0** | `N/A` |
| Quality | 12.34 % defect leakage | **81.7** | `N/A` |
| Security | 5.34 % pass rate | **3.8** | `N/A` |
| Throughput | 964 merged PRs | **100.0** | `0/100` |
| Impact | 326 feature issues | **100.0** | `0/100` |
| | | **DVI 76.2** | — |

Since the data resolves, the live `N/A` has to come from one of three things, in order of likelihood:

1. **Stale or absent `vf_dvi` doc** — the ETL hasn't run against this catalog recently.
2. **Mapping-group scope** emptying the cohort. An active mapping with no positive selector returns a
   *fully zeroed* snapshot with `historical: []` (`mongoConceptViewRepo.js:3979`) — indistinguishable
   from a seeding failure from the UI.
3. **The app resolving to a different catalog** than `playground_prod`. Still unverified — see §7.

The trend chart *did* have data in the screenshot, which argues against (2) and points at (1) or (3).

## 2. Three of five tiles saturate — the snapshot path can't produce a good demo

The `dviConfig.ts` thresholds don't match the units the ETL feeds them:

| Dimension | "Excellent" threshold | Actual raw value | Result |
|---|---|---|---|
| Throughput | 20 (story points) | 964 (merged PRs) | pinned at 100 |
| Impact | 12 (features) | 326 (issues) | pinned at 100 |
| Velocity | 4 (hours) | 0.32 (hours) | pinned at 100 |

Velocity's 0.32h median is an artefact: the org-wide PR union is dominated by `OpseraEngineering`,
`securefactory` and `opsera-agentic` rows whose cycle times are 0.02–0.5h. `demo-acme-direct`'s own
PRs are a realistic 52–118h, but they're a rounding error in the union.

**Consequence:** no amount of seeding makes the *snapshot* path interesting — 3 tiles read 100 and
Security reads ~4. Only the **individuals path** (`computeDviFromIndividuals`, active whenever
`individuals[]` is non-empty) produces a meaningful spread. This confirms and sharpens the Phase 1/2
priority: per-developer identity-joined rows are the whole ballgame, not org-level volume.

## 3. Security is structurally unfixable by seeding alone

Tier 1 (`asp_sonar_issues`) wins on **any** vulnerability row, with **no date and no org filter** —
whole table, always:

| type | status | rows |
|---|---|---|
| VULNERABILITY | OPEN | 744 |
| VULNERABILITY | CLOSED | 42 |

→ 42/786 = **5.34 %**, which normalizes to **3.8/100**.

Meanwhile tier 2 would give a healthy **95.7 %** (3304 `OK` of 3451 gates in-window; and
`demo-acme-direct`'s own gates are 141/150), and tier 3 gives 2888/8468 security-workflow runs ≈
**34 %**. **Neither ever runs.**

Adding `demo-acme-vf` vulnerabilities cannot fix this — to drag 42/786 up to ~95 % we'd need roughly
14,000 resolved-and-zero-open vulnerability rows, which is absurd seed volume. Real options:

- **Per-developer path.** Per-dev security uses GHA pass rate *per repo* (`gha-per-repo`) before
  falling back to the org number. Seeding `github_actions_runs_rest_api` rows for `demo-acme-vf`
  repos with high success rates gives our cohort a good per-dev security score even while the org
  number stays poor. **This is the practical lever.**
- **Product fix.** `aspSonarSecurityMetricsQuery` should be org- and date-scoped like every other
  dimension. Worth filing — it makes the Security tile meaningless on any multi-tenant catalog.
- Resolve the 744 open rows. Not ours to touch (other orgs' data).

## 4. Catalog-wide ingestion stopped ~2026-08-02 — and that's an opportunity

| Source | Last data |
|---|---|
| PR union | 2026-08 (1613 PRs vs 5734 in July) |
| `github_actions_runs_rest_api` | `max_insert = 2026-08-02` |
| `raw_mongo_pipelineactivities` | 2026-08 (5203 vs 184755 in July) |
| `v_itsm_issues_hist` | `max_created = 2026-08-28` |

**September 2026 is completely empty, catalog-wide.** The snapshot only falls back one month, so it's
currently reading a partial August.

This is a lever rather than a problem: if `demo-acme-vf` seeds **September 2026** PRs, it will be the
*only* org with current-month data, so the latest-month snapshot becomes **100 % ours** — Velocity and
Throughput get scoped to our cohort without needing a mapping group at all.

## 5. Two verified defects in our own generators

Both affect `demo-acme-direct` today, so they matter for vnxt as much as VisualForge.

### 5a. `base_datasets.commits_rest_api.commit_email` is never populated

`generators/commits.py:17-21` inserts `cleansed_user_name`, `cleansed_commit_author`, `user_id` — but
**not `commit_email`**. Confirmed empirically: every month of `demo-acme-direct` (37,598 rows) and
`demo-meridian` reports **`authors = 0`** distinct commit emails, while real orgs report 43–95.

The ETL reads `LOWER(TRIM(commit_email)) AS author_email` and then requires an `@` before a developer
is created (`validateDevIdentifier`, via `getOrCreateDev`). Our commits resolve only if a
login→email mapping happens to exist from another source (Bitbucket users, Jira users, or a commit row
that *does* carry an email). Fix is one column.

### 5b. ITSM priorities never match the high-priority defect set

`generators/itsm_issues.py:41` — `_PRIORITIES = ["high"]*2 + ["medium"]*5 + ["low"]*3`.

The Quality dimension counts a defect only when
`UPPER(issue_priority) IN ('BLOCKER','CRITICAL','HIGHEST','1','1 - CRITICAL')`. The catalog-wide 12.34 %
leakage comes entirely from **real** Opsera Jira rows (`HIGHEST` 78,571 · `BLOCKER` 15,664); our
`HIGH`/`MEDIUM`/`LOW` rows are all in the "ignored by Quality" bucket. So a mapping-scoped demo would
show 0 % leakage — reading as *perfect quality* rather than a real number.

## 6. Resolved risks

- **`master_data.date_dim` is fine** — 2018-12-30 → 2030-04-03, 4113 rows, no null
  `week_start_date` / `month_start_date`. The join-spine risk from the plan is closed.
- **`demo-acme-vf` is unused** — no collision in `pull_requests`, `commits_rest_api` or `commits_prs`.
- **5 missing tables, none blocking.** `github_action_jobs_rest_api`, `raw_mongo_pipelines`,
  `raw_jira_issues_rest_api`, `v_ghas_overview`, `team_member_list` are all absent, but each is read
  through a probe or `safeQuery` and degrades to a fallback. No Phase 2 DDL needed for the five tiles.
  (`raw_jira_issues_rest_api` missing simply means the ETL uses `v_itsm_issues_hist` — which is what
  we seed anyway.)
- **All three PR sources are readable** and `demo-acme-direct` has emails in
  `consumption_layer.commits_prs` (7601 rows), so PRs already resolve to developers even though
  commits don't.
- **`v_github_teams_members_current`** has 100 `demo-acme-direct` rows, so team labels work.

## 7. Still open

1. **Which catalog does the VisualForge app resolve to?** The notebook session reported
   `productionworkspace_us_east_2` / `default`, which tells us nothing about the app. Needs
   `GET /api/v1/databricks/verify-catalog` or the `DATABRICKS_CATALOG` value from
   `unified-backend/.env`. **If it isn't `playground_prod`, everything above describes the wrong
   catalog.** This is now the single highest-priority unknown.
2. **Has `POST /etl/sync/dvi` ever run against this catalog?** Check `GET /etl/status` and the
   `vf_dvi` doc's `syncedAt`. If it's stale, the UI may already improve with no seeding at all.
3. **Is a mapping group active on the demo tenant, and does it carry selectors?**

## 8. Revised plan implications

| Plan item | Status after Phase 0 |
|---|---|
| Phase 0 diagnose | **Done**, except the app-side catalog check (§7.1) |
| "Seed the 5 dimensions to kill N/A" | **Wrong framing** — they already resolve; fix scope/ETL first |
| Per-developer `individuals[]` rows | **Promoted to the primary goal** (§2) |
| Seed current-month PRs | **Confirmed critical**, and now a scoping shortcut (§4) |
| `date_dim` verification | **Closed** — no work needed |
| Missing-table DDL | **Dropped** — none of the 5 blocks the tiles |
| Security via Sonar/ASP seeding | **Replaced** by the GHA-per-repo per-dev route (§3) |
| Generator fixes | **New work** — `commit_email` and ITSM priorities (§5) |
