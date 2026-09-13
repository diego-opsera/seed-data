# DVI Dashboard — Seeding Plan (VisualForge)

Target: `/app/dashboard?concept=dvi` — Developer Velocity Index — currently showing
`N/A` on Velocity / Quality / Security, `0/100` on Throughput / Impact, `-100.0 vs baseline`
on all five, and `0.0 Critical` across the DVI Projection.

Decisions taken (2026-09-13):
- **Separate demo org** — all VisualForge seeding uses a new org identifier, not `demo-acme-direct`.
- **Full scope** — five dimension tiles + per-developer individuals + 12-month trend + projection.

Source of truth for everything below: `visualforge@b8377ed22`,
`services/unified-backend/src/modules/integration/databricks/`.
See also [visualforge_seeding_insights.md](visualforge_seeding_insights.md).

---

## 1. How the DVI dashboard actually gets its numbers

```
Databricks (playground_prod)
  → syncDvi()                        etl/syncDvi.js (2498 lines)
  → vf_dvi Mongo doc, _key: 'latest'  one global doc, NOT per-tenant
  → GET/POST /api/v1/databricks/dvi   getDviFiltered() applies mapping scope
  → useDatabricksDvi()                frontend/src/dashboard/dvi/useDatabricksDvi.ts
  → computeDvi() / computeDviFromIndividuals()
  → tiles
```

Four facts that drive the whole plan:

**(a) Two competing tile paths.** `DviConceptDashboard.tsx:1051` — `result = individualsResult ?? snapshotResult`.
When `individuals[]` is non-empty the tiles show **avg(per-developer dimension scores)**; only when it is
empty do they fall back to the org-level `snapshot`. So seeding must satisfy the *individuals* path to
control what's displayed — which means every fact row needs a **developer email that resolves**.

**(b) The snapshot tiles are latest-month-only.** `syncDvi.js:1233-1240`:
`latestPr = prByMonth.get(latestMk) ?? prByMonth.get(prevMk)`, and PRs are bucketed by
**`created_at`** (`syncDvi.js:593`). With today = 2026-09-13, Velocity and Throughput read *only PRs
created in 2026-09* (or 2026-08 if September is empty). Seeding 12 months of history but nothing in the
current month leaves those two tiles at 0.

**(c) `syncDvi` has zero org filtering.** Grep for `org_name` in `syncDvi.js` → 0 hits. Several queries
(`aspSonarSecurityMetricsQuery`, `sonarIssuesQuery`, `itsmDefectLeakageQuery`) have no `WHERE` on org at
all. The ETL aggregates **the entire catalog** into one `vf_dvi` doc. Isolation happens at serve time in
`getDviFiltered()` (`mongoConceptViewRepo.js:3963`) via `members` / `repos` / `jiraProjects` / `orgName` /
`mappingGroups`.

> **Caveat on the separate-org decision:** a new org identifier gives us clean, safely-deletable rows
> and guarantees we never mutate what vnxt reads — but it does **not** isolate the DVI dashboard by
> itself. The demo must run with an **active mapping group** scoped to the new org's repos / Jira
> projects / member emails, otherwise the tiles blend our data with every other org in `playground_prod`.
> Building that mapping group is a required deliverable, not an optional extra (Phase 4).

**(d) `available: false` → `N/A`.** `DviConceptDashboard.tsx:1902`. Only Quality and Security carry an
`available` flag through `toSnapshot()` (`useDatabricksDvi.ts:44-48`), set from
`qualitySource !== 'none'` / `securitySource !== 'none'` in the ETL. Velocity's `N/A` is **not**
reproducible from this checkout — with `withAI: 0` and no `available` flag, `normalizeScore(0)` on an
inverted metric returns 100, so the tile should read `100`, not `N/A`. Either the deployed build differs
from `b8377ed22`, or the individuals path is active with an empty cohort. Phase 0 settles this
empirically rather than guessing.

---

## 2. Per-tile data contract

Weights (both ETL and frontend): **Velocity 0.25 · Quality 0.25 · Security 0.20 · Throughput 0.15 · Impact 0.15**.
`normalizeDimension(raw, lower, upper, inverted)` = clamp → linear scale → ×100 (`syncDvi.js:87`).

### Velocity — "PR Cycle Time · GitHub"
| | |
|---|---|
| Snapshot | `median(cycleHours)` of PRs created in the latest month; `cycleHours = (merged_at − created_at)/3600s`, kept only when `0 ≤ h < 10000` |
| Per-dev | `normalizeDimension(median(dev.cycleHours), 1, 48, inverted)` |
| Tables | `source_to_stage.raw_github_pull_requests_rest_api_prs` (+`_details`) — preferred; **`base_datasets.pull_requests`** and **`consumption_layer.commits_prs`** also work |
| Contract | `pr_id`, `user_login`/author email, `created_at`, `merged_at`, `repository`, `state` |
| Gotchas | Bucketed by `created_at`. `isBot(login)` drops `*[bot]`, `dependabot`, `github-actions`, `renovate`, `web-flow`, `copilot*`, `*-bot`. Cycle time of exactly 0h is kept; negative is dropped |

**All PR sources are queried and merged** — `sharedToolLoader.js:861-867` loops every entry in
`PR_SOURCES` and dedups on `pr_id:repository`, then supplements from `consumption_layer.commits_prs`
(`:912`). It is *not* first-source-wins, so our existing `base_datasets.pull_requests` rows already
contribute. Column mapping for that fallback is `rawLegacyBaseDatasetPrsQuery` in
`computeVelocityQueries.js`: `merge_request_id`, `pr_created_datetime`, `pr_merged_datetime`,
`pr_commits[0].cleansed_author_login`, `merge_status`, `pr_state`, `project_url`, `org_name`.

### Quality — "Defect leakage to production · Jira"
| | |
|---|---|
| Formula | `high_priority_defects / total_issues × 100` (inverted — lower is better) |
| Table | `base_datasets.v_itsm_issues_hist` → fed by our `transform_stage.mt_itsm_issues_current` |
| Query | `itsmDefectLeakageQuery` (`dviQueries.js:659`) — filtered on `issue_created_date` only, **no org filter** |
| Defect test | `LOWER(issue_type) IN ('bug','defect')` **AND** `UPPER(issue_priority) IN ('BLOCKER','CRITICAL','HIGHEST','1','1 - CRITICAL')` |
| Availability | `qualitySource = 'none'` (→ `N/A`) when `total_issues = 0` |

**Known mismatch:** `generators/itsm_issues.py` emits `issue_priority` values `high` / `medium` / `low`.
**None of those match** the required set, so `high_priority_defects` is always 0 → 0% leakage → the tile
would read *perfect quality* rather than a real number. The new generator must emit `CRITICAL` /
`BLOCKER` / `HIGHEST` for the defects we want counted.

### Security — "Security Scan Pass Rate · SonarQube"
Three-tier fallback (`syncDvi.js:1110-1126`):
1. **`base_datasets.asp_sonar_issues`** — `resolved_vulnerabilities / total_vulnerabilities × 100`;
   needs `UPPER(type)='VULNERABILITY'` and `UPPER(status) IN ('RESOLVED','CLOSED')`. **No date filter
   and no org filter at all** (`aspSonarSecurityMetricsQuery`) — whole table, always.
2. `source_to_stage.raw_sonar_metric_split_data_branchwise` — `quality_gate_status = 'OK'` ÷ row count.
   This one *is* date-filtered on `last_analysis_date` and also supplies the **per-month** security
   series for the trend (`sonarByMonth`).
3. `source_to_stage.github_actions_runs_rest_api` — workflow runs whose `message.name` matches
   `%security%|%scan%|%codeql%|%sast%|%dast%|%snyk%|%trivy%|%lint%|%test%|%quality%`, success ÷ total.

Per-developer security uses `sonarByAuthor` (`raw_sonar_type_data_branchwise` author summary) and falls
back to the org score, so per-dev Sonar author rows are needed for variation across the cohort.

### Throughput — "Story points completed / sprint · Jira"
**The label lies.** `snapshot.throughput.withAI = latestPr.mergedPrs` (`syncDvi.js:1237`) — merged PR
count for the latest month, not story points. Per-dev throughput normalizes **commits**, not PRs.
`metricValues` never emits a `story-points-per-sprint` key (confirmed: grep of all 24
`metricValues[...]` assignments), so selecting that metric explicitly yields `{withAI: 0}`.
To move this tile: **merged PRs in the current month** plus per-dev commits.

### Impact — "Features shipped to production · Jira"
| | |
|---|---|
| Snapshot | `Σ feature_issues` from `jiraIssueCycleTimeQuery`, falling back to the latest month's deploy count |
| Per-dev | `_featureIssues`, falling back to `mergedPrs`, then `prs` |
| Trend | **deploy counts**, *not* feature issues (`syncDvi.js:1337`) |
| Feature test | `LOWER(issue_type) IN ('story','feature','new feature','enhancement','epic','user story','product backlog item')` |
| Also required | `issue_resolution_date IS NOT NULL`, `issue_updated_date` in window, `itsm_source='jira'`, and `LOWER(issue_status)` in the completion set: `done, closed, resolved, completed, complete, released, ready for production, ready for release, approved for deploy, product deployment, fixed` (`completionStatuses.js:13`) |

Deploy counts come from `source_to_stage.raw_mongo_pipelineactivities` (preferred) or, when that's
empty, deploy-filtered `github_actions_runs_rest_api` workflows (`syncDvi.js:1041-1076`).

### Trend chart + projection
Per month (`syncDvi.js:1310-1342`):
```
vel  = normalizeDimension(median(cycleHours_of_PRs_created_that_month), 1, 48, inverted)   // 0 if none
thru = normalizeDimension(mergedPrs × runRate, 0, max(P90_monthly_merged_prs, 50), false)
sec  = normalizeDimension(monthSecRate, 30, 100, false)   // sonarByMonth → GHA → org rate
qual = normalizeDimension(qualityRaw, 30, 100, false)     // ORG-LEVEL CONSTANT, same every month
imp  = normalizeDimension(deploys × runRate, 0, max(P90_monthly_deploys, 100), false)
withAI    = vel×0.25 + qual×0.25 + sec×0.20 + thru×0.15 + imp×0.15
withoutAI = withAI × 0.82
```
Notes that shape the generator:
- **Quality is flat across the trend** — one org-wide number replayed into every month. Only Velocity,
  Throughput, Security and Impact can make the line move.
- `runRate = daysInMonth / dayOfMonth` for the in-progress month, so a partial September is scaled up
  ~3.3× on 2026-09-13. Seed September at true run-rate or the last point spikes.
- Throughput and Impact are **self-normalizing against their own P90**. A flat 12 months of identical
  PR/deploy volume produces a flat ~90-100 on both; the shape of the story has to come from *relative*
  month-over-month variation, not absolute volume.
- The projection is pure frontend arithmetic off `uplift` (`dviCompute.ts:projectDviSeries`,
  clamped to 0.4–4.5 pts/month). `0.0 Critical` today is just `effectiveDviScore = 0` — it needs no
  seeding of its own and will light up automatically once the tiles do.
- `historicalDaily` / `historicalWeekly` / `historicalBiweekly` are separate arrays selected by the
  `granularity` param, so day/week presets need daily-resolution rows, not just monthly totals.

---

## 3. Identity: the single highest-risk item

For a developer to appear in `individuals[]` and for Quality/Impact to attach to them, **the same email
must appear across every source**: GitHub commits + PRs, Jira `assignee_email`, Sonar `author`, and AI
tool usage. Joins are on `LOWER(TRIM(email))`.

Gates in `etl/sharedIdentity.js` our synthetic roster must survive:

| Gate | Rule | Our roster |
|---|---|---|
| `isBot` | drops `*[bot]`, `dependabot`, `github-actions`, `renovate`, `web-flow`, local part `agent`/`copilot`/`cursoragent`, `*-bot` | ✅ `demo-alice`, `demo-user-001` clean |
| `isRealUserEmail` | must contain `@`, must not end `@github-member.local` | ✅ |
| `hasLikelyRealEmailLocalPart` | local part ≤12 chars auto-passes; longer must not look hashed | ⚠️ `demo-user-001` is 13 chars — passes only because it contains `-`; avoid long separator-free logins |
| `isLikelyHumanLogin` | no `svc` / `service` / `admin` substring, not all-digits, `^[a-z0-9._-]{3,}$` | ✅ — **do not** name any demo user `*admin*` or `*service*` |
| `isLikelySyntheticDisplayName` | drops names whose first token is a long hex/opaque token, or contains `{gitusername}` | ✅ |

Team labels come from `loginToTeam` (GitHub teams members), defaulting to `'Engineering'` — so
`v_github_teams_members_current` rows are needed for Team Split to show real team names. Note
`BUGS.md` §2: that view's global-MAX-timestamp pattern means a newer `record_update_datetime` on our
rows can hide every other org's members. With a separate demo org this is still a shared-view risk and
must be checked after insert.

---

## 4. Phased plan

### Phase 0 — Diagnose before writing anything (half a day)
Nothing here is speculative work; it decides the rest of the plan.

1. **Confirm which catalog the VisualForge demo tenant resolves to.** `GET /api/v1/databricks/verify-catalog`,
   or `SELECT current_catalog()`. If it isn't `playground_prod`, every table in §2 is the wrong target
   and the plan pauses. (Per-user Opsera scope resolution — `queryHelpers.js:resolveCatalogPrefixForUser`.)
2. **Probe each DVI source with the ETL's exact predicates** — `vf/notebooks/dvi/diag_sources.py`:
   - `raw_github_pull_requests_rest_api_prs`, `base_datasets.pull_requests`, `consumption_layer.commits_prs`
     — row counts by month on `created_at` / `pr_created_date`, **especially 2026-09**
   - `v_itsm_issues_hist` — counts by `itsm_source`, plus the distinct `issue_priority` and
     `issue_status` values actually present (verifies the `high`-vs-`CRITICAL` mismatch)
   - `asp_sonar_issues` — counts by `UPPER(type)` / `UPPER(status)`
   - `raw_sonar_metric_split_data_branchwise` — counts by `quality_gate_status` and month
   - `github_actions_runs_rest_api`, `raw_mongo_pipelineactivities` — existence + row counts
   - `master_data.date_dim` — `MIN/MAX(calendar_date)`, confirm it spans 2025-09 → 2026-09
3. **Read the ETL's own diagnosis.** `syncDvi` logs every resolution decision
   (`[syncDvi] Security: source=… passRate=…`, `[syncDvi] Quality: source=… (ITSM: total=… defects=…)`,
   `[syncDvi] ITSM sources resolved: …`, `[syncDvi][reconcile] commitRows=… prRows=…`). One ETL run with
   logs captured answers in minutes what probing answers in hours. Also `GET /etl/status` and
   `scripts/validateDviDashboard.mjs`.
4. **Settle the Velocity `N/A`** (§1d) by inspecting the live `vf_dvi` doc: is `individuals` empty, and
   does `snapshot.velocity` carry an `available` key?

**Exit criteria:** a table-by-table verdict of *absent / present-but-empty / present-but-wrong-shape /
usable*. The three scripts that produce it (all read-only, safe to re-run):

```python
exec(open("/tmp/seed-data/vf/notebooks/dvi/diag_catalog.py").read())     # schema + table reachability
exec(open("/tmp/seed-data/vf/notebooks/dvi/diag_sources.py").read())     # row counts, windows, value sets
exec(open("/tmp/seed-data/vf/notebooks/dvi/diag_dimensions.py").read())  # replays the ETL, predicts tiles
```

Record the output in `docs/vf_dvi_phase0_findings.md` (same role `docs/exploration.md` played for the
original Copilot batch).

### Phase 1 — Org + roster foundation
- Add `demo-acme-vf` (id `9990003`) to `vf/config/entities.yaml`, with its own repos
  (`demo-acme-vf/{backend,frontend,api-gateway,platform}`) and Jira project key.
- New `vf/config/stories/dvi.yaml` narrative: a 12-month arc with deliberate month-over-month movement in
  cycle time, merged-PR volume, gate pass rate and deploy counts (remember: Quality can't move the trend).
- Roster of ~25-40 developers with stable `login` + `email`, emails reused verbatim by every generator
  in Phase 2. Emails are the join key — one typo silently drops a developer from `individuals[]`.
- Verify each login/email against the five gates in §3.

### Phase 2 — Generators (`vf/generators/`)
New package so nothing vnxt reads is touched. Every generator scopes deletes to the new org.

| # | Generator | Table | Feeds |
|---|---|---|---|
| 1 | `vf_github_prs.py` | `source_to_stage.raw_github_pull_requests_rest_api_prs` + `_details` | Velocity, Throughput, trend |
| 2 | `vf_github_commits.py` | `source_to_stage.raw_github_commits_rest_api` | per-dev throughput, `individuals` |
| 3 | `vf_itsm.py` | `transform_stage.mt_itsm_issues_current` (new-org rows only) | Quality, Impact — with `CRITICAL`/`BLOCKER` priorities and completion-set statuses |
| 4 | `vf_sonar.py` | `raw_sonar_metric_split_data_branchwise` + `raw_sonar_type_data_branchwise` + `asp_sonar_issues` | Security (tiers 1+2), per-dev security, monthly security series |
| 5 | `vf_gha_runs.py` | `github_actions_runs_rest_api` + `github_action_jobs_rest_api` | Security tier 3, deploy counts → Impact trend |
| 6 | `vf_teams_members.py` | `raw_github_teams_members` (new-org rows) | Team Split labels |
| 7 | `vf_pipeline_activities.py` | `raw_mongo_pipelineactivities` | preferred deploy source for Impact |

Table DDL check first — items 1, 2, 5, 7 are tables our generators have never written;
`vf/notebooks/dvi/create_tables.py` may be needed (Phase 0 step 3 reports exactly which). **Note the `message` column shape:** GHA queries parse it with
`GET_JSON_OBJECT(message, '$.name' / '$.conclusion' / '$.status')`, so those rows carry a JSON string
payload, not flat columns.

Non-negotiable across all seven: PRs **created and merged in the current calendar month** at a
believable run-rate, and one consistent email per developer.

### Phase 3 — Run + ETL
`vf/notebooks/dvi/insert.py` (+ `delete.py` scoped to the new org), then — and this is the step that has no
vnxt equivalent — **trigger the ETL**:
```
POST /api/v1/databricks/etl/sync/dvi        { months: 12 }
```
Nothing appears in the UI until this runs. In dev the scheduler is inert (`NODE_ENV` gate), and there's
a process-wide ETL lock, so this serializes against any other sync.

### Phase 4 — Mapping group (required, per §1c)
Create a VisualForge mapping group scoped to the new org — repos, Jira project, member emails — so
`getDviFiltered()` returns only our cohort. Watch the trap: a mapping that is *active but carries no
selector* returns a **fully zeroed snapshot with `historical: []`** (`mongoConceptViewRepo.js:3979`),
which looks exactly like a seeding failure. Also seed `vf_user_mappings` date ranges covering the full
activity window — activity outside the range counts as **zero, not missing**.

### Phase 5 — Verify
- `node scripts/dev/validate-dviDashboard.mjs` / `scripts/validateDviDashboard.mjs` (theirs, 331 lines)
- Inspect the `vf_dvi` doc: `snapshot`, `individuals.length`, `historical.length`,
  `securitySource`, `qualitySource`, `itsmSource` — these name the source each dimension resolved to,
  which is the only way to prove the tiles read *our* rows and not a fallback
- Walk the UI: five tiles non-`N/A`, Individual View populated, Team Split grouped, trend shaped as the
  story intends, projection non-zero

---

## 5. Risks

| Risk | Mitigation |
|---|---|
| Demo tenant's catalog isn't `playground_prod` | Phase 0 step 1 — blocks everything, check first |
| No mapping group → our rows blend with other orgs, or an empty-selector mapping zeroes everything | Phase 4; verify both the scoped and unscoped reads |
| `master_data.date_dim` doesn't cover the window → silently empty trend | Phase 0 step 2; date spine is join-critical |
| `v_github_teams_members_current` global-MAX pattern hides other orgs after our insert | `BUGS.md` §2 — re-check the view for all orgs post-insert |
| Tenant id contains "playground" → AI-assistant fixtures override seeded data | Doesn't affect the five DVI dimensions, but does affect the AI Tooling panel on the same dashboard |
| Velocity `N/A` mechanism still unexplained | Phase 0 step 4 — inspect the live doc rather than infer from source |
| Throughput label says story points but reads merged PRs | Seed merged PRs; flag the label as a product bug for `BUGS.md` |
