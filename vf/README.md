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
    entities.yaml          Phase 1 — demo-acme-vf org, repos, roster
    stories/dvi.yaml       Phase 1 — 12-month DVI narrative
  generators/              Phase 2 — one module per Databricks table
  notebooks/
    dvi/
      diag_catalog.py      Phase 0 — catalog + schema reachability
      diag_sources.py      Phase 0 — per-table verdict for every DVI source
      diag_dimensions.py   Phase 0 — replays the ETL's own dimension logic
```

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
