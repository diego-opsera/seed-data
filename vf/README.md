# `vf/` — VisualForge seed data

Everything in this tree targets the **VisualForge** front end
(`/Users/diegovillafuerte/Documents/Github/visualforge`, repo `securefactory/visualforge`).

The rest of this repo targets **vnxt-insights-api**, which is maintained in parallel. The two front
ends read overlapping Databricks tables, so this tree is kept separate on purpose:

- **Separate demo org.** VisualForge data uses `demo-acme-vf`; vnxt batches own `demo-acme-direct` and
  `demo-acme-engineering`. Neither front end ever reads the other's rows.
- **Separate config.** `vf/config/` has its own `entities.yaml` + story, so editing the VisualForge
  roster can't shift a vnxt dashboard.
- **Shared helpers only.** Generators here import `generators.utils` (dates, story loading, user
  expansion) rather than forking it. Nothing in `vf/` writes to a table a vnxt generator owns.

Background and the full plan: [`docs/visualforge_seeding_insights.md`](../docs/visualforge_seeding_insights.md)
and [`docs/dvi_seeding_plan.md`](../docs/dvi_seeding_plan.md).

---

## Layout

```
vf/
  config/
    entities.yaml          demo-acme-vf org, 4 teams, 5 repos, 25-dev roster
    stories/dvi.yaml       DVI narrative — dimension targets + forward-dating
  generators/
    identity_gates.py      port of VisualForge's identity gates + roster check
    story.py               story loading, forward-dating, roster accessors
                           (Phase 2 table generators land here)
  notebooks/
    dvi/
      diag_catalog.py      Phase 0 — catalog + schema reachability
      diag_sources.py      Phase 0 — per-table verdict for every DVI source
      diag_dimensions.py   Phase 0 — replays the ETL's own dimension logic
```

## Identity is the contract

VisualForge joins commits, PRs, Jira, Sonar and AI usage on `LOWER(TRIM(email))`, and drops any row
whose identity fails `sharedIdentity.js`. A failure is **silent** — the developer just never appears in
`individuals[]`, which is the path that drives every dashboard tile once it is non-empty.

So the roster in `config/entities.yaml` carries an explicit `email` per developer, and
`generators.story.roster()` refuses to return it if any entry would be dropped. Validate any time:

```bash
python3 -m vf.generators.identity_gates      # prints PASS/FAIL per developer
python3 -m vf.generators.story               # prints the arc, roster and repo mapping
```

This is not theoretical: `generators/commits.py` never populates `commit_email`, so all 37,598
`demo-acme-direct` commits already resolve to zero developers (BUGS.md #12).

## Why the story seeds into the future

`playground_prod` is a one-time copy from real prod with an ~2026-08-02 cutoff. The DVI snapshot falls
back exactly one month while `buildMonthKeys` always ends at the current month, so from 2026-10-01 both
October and September are empty and Velocity/Throughput zero out on their own.

`load_dvi_story()` therefore pushes `end_date` past today by the story's `forward_days`. Every ETL query
is bounded by `TO_DATE = currentDate()`, so future-dated rows stay invisible until their month arrives —
the demo keeps working as the calendar advances instead of needing a monthly re-seed.

## Running

The repo is synced to `/tmp/seed-data` on the Databricks cluster (`notebooks/clone.sh`). Invoke from a
notebook cell:

```python
exec(open("/tmp/seed-data/vf/notebooks/dvi/diag_catalog.py").read())
exec(open("/tmp/seed-data/vf/notebooks/dvi/diag_sources.py").read())
exec(open("/tmp/seed-data/vf/notebooks/dvi/diag_dimensions.py").read())
```

All three Phase 0 scripts are **read-only** — no INSERT, UPDATE, DELETE or CREATE. Safe to run against
`playground_prod` at any time.

## Catalog safety

`CATALOG = "playground_prod"` in every script. Nothing in this tree references another catalog.

## Important: seeding Databricks is only half the job

VisualForge reads its dashboards from MongoDB snapshots, not from Databricks directly. After any
insert, the ETL has to run or the UI shows nothing:

```
POST /api/v1/databricks/etl/sync/dvi     { "months": 12 }
```

And because `syncDvi` applies **no org filter** (one global `vf_dvi` doc covers the whole catalog),
the dashboard additionally needs an active mapping group scoped to `demo-acme-vf` for our rows to be
the ones on screen. See Phase 4 of the plan.
