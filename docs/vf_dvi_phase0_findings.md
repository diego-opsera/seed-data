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

## 4. The catalog is a one-time prod copy with an ~2026-08-02 cutoff — so the dashboard decays

| Source | Last data |
|---|---|
| PR union | 2026-08 (1613 PRs vs 5734 in July) |
| `github_actions_runs_rest_api` | `max_insert = 2026-08-02` |
| `raw_mongo_pipelineactivities` | 2026-08 (5203 vs 184755 in July) |
| `v_itsm_issues_hist` | `max_created = 2026-08-28` |

This is **not** broken ingestion — `playground_prod` is a one-time copy from real prod (confirmed
2026-09-13), so the cutoff is by design and the data is accurate as a static dataset. But it never
advances, and that has a dated consequence.

### The dashboard goes dark on 2026-10-01 by itself

`buildMonthKeys(12)` always ends at the **current** month (`queryHelpers.js:129`), and the snapshot
falls back exactly **one** month: `latestPr = prByMonth.get(latestMk) ?? prByMonth.get(prevMk)`
(`syncDvi.js:1233`).

| Date | `latestMk` | `prevMk` | Velocity / Throughput |
|---|---|---|---|
| today (2026-09-13) | 2026-09 — empty | 2026-08 — has data | works |
| from 2026-10-01 | 2026-10 — empty | 2026-09 — empty | **0** |

So Velocity and Throughput zero out on 1 October regardless of anything we seed, and the last two
trend buckets flatten to 0. Any demo built on the copied data has ~18 days of life left.

### Which makes forward-dating the seed the fix, not just a nicety

`demo-acme-vf` should seed **through at least 2026-12**, not just up to today. Every ETL query is
bounded by `TO_DATE = currentDate()`, so future-dated rows are simply excluded until their month
arrives — harmless now, and the demo keeps working as the calendar advances instead of needing a
re-seed every month.

It also hands us a free scoping win: because nothing else in the catalog has data after 2026-08-02,
our rows will be the **only** ones in the latest-month snapshot. Velocity and Throughput become
effectively cohort-scoped without a mapping group at all.

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

1. ~~Which catalog does the app resolve to?~~ **Closed 2026-09-13 — `playground_prod` confirmed.**
   Everything above describes the right catalog.
2. ~~Has the DVI ETL run?~~ **Not an action item.** The backend registers its own scheduler
   (`routes/index.routes.js:197`): `syncAllConceptViews` — which includes `syncDvi` — fires 30s after
   boot and then daily at `ETL_SYNC_HOURS` (default **07:00 and 19:00 UTC**), per tenant, with each
   tenant's own resolved catalog prefix. Gated on `NODE_ENV=production|test` and
   `DATABRICKS_SYNC_INTERVAL_MS !== '0'`. So seeded rows reach Mongo on the next scheduled run with no
   manual trigger — provided the demo environment runs with `NODE_ENV=production`.
   *(Note: `ETL_SCHEDULER_GUIDE.md` says this lives in `app.js:153-199`. It does not — that file has
   no scheduler. The real implementation is `src/routes/index.routes.js`.)*
3. **Is a mapping group active on the demo tenant, and does it carry selectors?** Still open.
4. **Does the demo environment run `NODE_ENV=production`?** If it's a dev instance, no scheduler
   registers and nothing we seed will ever reach the dashboard.

## 8. Revised plan implications

| Plan item | Status after Phase 0 |
|---|---|
| Phase 0 diagnose | **Done.** Catalog confirmed as `playground_prod` |
| "Seed the 5 dimensions to kill N/A" | **Wrong framing** — they already resolve; fix scope/ETL first |
| Per-developer `individuals[]` rows | **Promoted to the primary goal** (§2) |
| Seed current-month PRs | **Confirmed critical** — and extend forward through 2026-12, because the static copy makes the dashboard zero out on 2026-10-01 (§4) |
| `date_dim` verification | **Closed** — no work needed |
| Missing-table DDL | **Dropped** — none of the 5 blocks the tiles |
| Security via Sonar/ASP seeding | **Replaced** by the GHA-per-repo per-dev route (§3) |
| Generator fixes | **New work** — `commit_email` and ITSM priorities (§5) |

---

## 9. Phase 2 smoke-test results (2026-09-13)

First seed of `demo-acme-vf` — September 2026, via `vf/notebooks/dvi/insert.py`.

### The identity join works

| check | result |
|---|---|
| commit emails (distinct) | **25** |
| commits with null email | **0** |
| PR logins resolving to a commit email local part | **25 / 25** |
| PRs merged, median cycle | 273, **17.00h** |
| PRs gated out by the insert-date gate | **0** |
| GHA pass rate per repo | 97.4 – 100% |

So the `commit_email` fix (BUGS.md #12) does what it needed to: 25 developers
resolve where `demo-acme-direct`'s 37,598 commits resolve to none.

### Two things the seed got wrong

**a) The ETL window ends TODAY, so a forward-dated month is half-invisible.**
The smoke run seeded all of September but `TO_DATE = currentDate()`, so only
issues created on or before the 13th counted: `total_issues` read **9 of 20**
inserted, and both defects happened to fall after the 13th, giving
`high_priority_defects = 0`. Forward-dating is still correct (§4) — but
verification output has to be read as *"rows up to today"*, not *"rows seeded"*.

**b) Driving ITSM volume from `features_per_month` starved the org.**
Working backwards from Impact's threshold (excellent = 12 resolved features)
produced **20 issues a month for 25 developers**: 5 people had no Jira issue at
all, and only 2 defects existed, so per-developer Quality and Impact were noise.

The trade isn't real. Impact cannot track its curve for a realistic team — 25
developers completing two features each is 50/month against a threshold of 12 —
so **Impact saturates at 100 exactly like Throughput**, and the per-developer
path (normalized against the org P90) is what carries the spread. Volume now
comes from `issues_per_dev_per_week`: ~396 issues/month, all 25 developers
covered, 18 high-priority defects on or before today, leakage ≈ 8%.

### Predicted tiles after the fix

| dimension | raw | score |
|---|---|---|
| Velocity | 16.0 median cycle hours | **62.5** |
| Quality | ~8% defect leakage (our cohort) | ~93 |
| Throughput | 254 merged PRs | 100 (saturated) |
| Impact | 254 resolved features | 100 (saturated) |
| Security | per-repo GHA 97-100% | per-dev route only; org number still pinned at 3.8 |

Only Velocity lands mid-curve. That is a property of the tile design, not of the
seed: three of the five dimensions are COUNT metrics with thresholds calibrated
for something much smaller than a real team.

---

## 10. Phase 2/3 complete — Databricks side verified (2026-09-13)

Final smoke-window state for `demo-acme-vf`, September 2026:

| table | rows |
|---|---|
| `source_to_stage.raw_github_commits_rest_api` | 2,061 |
| `source_to_stage.raw_github_pull_requests_rest_api_prs` | 250 |
| `source_to_stage.github_actions_runs_rest_api` | 951 |
| `transform_stage.mt_itsm_issues_hist` / `_current` | 395 each |
| `source_to_stage.raw_github_teams_members` | 25 |
| `master_data.github_copilot_orgs_mapping` | 1 |

Verified against the ETL's own predicates (`verify_scoped.py`):

- **Identity joins cleanly** — 25 commit emails, 25 ITSM assignee emails, 25 PR
  logins, with 25/25 overlap on both joins and zero null emails.
- **Velocity** 17.00h median cycle on 250 merged PRs, 0 rows lost to the
  insert-date gate.
- **Quality** 162 in-window issues, 9 high-priority defects, 5.56% leakage.
- **Impact** 47 resolved features in the current month.
- **Security** per-repo GHA pass rates 97.4–100%, 54–58 security-gate runs per repo.
- **Team labels** without collateral damage — `v_github_teams_members_current`
  still shows `opsera-it-networking` 5,190 and `demo-acme-direct` 100 alongside
  our 25, so the global-MAX trap (BUGS.md #2) was avoided by matching the
  existing max exactly rather than writing a newer timestamp.

**The seed is also reproducible now.** `seeded()` originally used Python's
`hash()`, whose string hashing is salted per process, so identical inputs gave
different data each run (row counts drifted 2082 → 2076 → 2055).
`sqlutil.stable_seed()` (md5) fixes it. The shared `generators/` modules still
have this — `generators/commits.py:85` hashes a str — so a vnxt re-seed is not
byte-reproducible either.

### What is still unverified

Everything above is the Databricks half. Three things can only be confirmed from
the application side:

1. Does the scheduled ETL run (requires `NODE_ENV=production` on the demo env)?
2. Does `individuals[]` populate — i.e. do the 25 developers survive the ETL's
   identity resolution end to end?
3. Is a mapping group scoped to `demo-acme-vf`, so the tiles read our cohort
   rather than the whole catalog?

Until (2) is confirmed there is no point seeding the full 16-month arc.
