# AI Code Comparison dashboard — seed batch

Seeds the consumption-layer tables + filter rows needed by the AI Code
Comparison dashboard (`/insights/ai-code-comparison`).

## Story

Three AI coding assistants compared head-to-head on the same Acme org:

| Tool          | Allocation | Rollout offset | Active share | Productivity mult | Chat share |
|---------------|-----------:|---------------:|-------------:|------------------:|-----------:|
| GitHub Copilot|        100 |     day 0      |        0.75  |              1.00 |       0.35 |
| Cursor        |         40 |    day +90     |        0.80  |              1.25 |       0.55 |
| Claude Code   |         15 |    day +210    |        0.85  |              1.65 |       0.70 |

Story arc the data tells:
- **Copilot** — incumbent. Steady high adoption, baseline per-user productivity, more IDE-inline than chat.
- **Cursor** — adopted ~3 months in. Smaller cohort but 25% higher per-user productivity and more chat-driven workflows.
- **Claude Code** — newest, ~7 months in. Smallest cohort with the highest per-user productivity (+65%) and the most chat-heavy usage pattern.

Tool roster is configured in `config/entities.yaml` under `ai_tools`. All
license/active-share curves ramp linearly over 30 days from rollout offset,
then plateau.

## Tables seeded

### New tables (this batch owns them end-to-end)
| Table                                                       | Generator                                  |
|-------------------------------------------------------------|--------------------------------------------|
| `consumption_layer.ai_assistant_license_info`               | `ai_assistant_license_info.py`             |
| `consumption_layer.ai_assistant_user_engagement`            | `ai_assistant_user_engagement.py`          |
| `consumption_layer.ai_assistant_programming_language_agg`   | `ai_assistant_programming_language_agg.py` |
| `consumption_layer.ai_assistant_language_model_metrics`     | `ai_assistant_language_model_metrics.py`   |
| `consumption_layer.commits_prs`                             | `commits_prs.py`                           |
| `master_data.filter_values_unity` (3 `tool_type` rows)      | inline in `insert.py`                      |

### Shared tables (this batch ADDS non-copilot rows only)
| Table                                                  | This batch writes                | direct/ writes              |
|--------------------------------------------------------|----------------------------------|-----------------------------|
| `consumption_layer.ai_assistant_acceptance_info`       | cursor + claude code rows        | github copilot rows         |
| `consumption_layer.ai_code_assistant_usage_user_level` | cursor + claude code rows        | github copilot rows         |

The two existing generators (`generators/ai_assistant_acceptance.py`,
`generators/ai_usage_user_level.py`) are unchanged — they still emit
github copilot rows only. The new generators here
(`ai_compare_acceptance.py`, `ai_compare_usage_user_level.py`) filter the
`ai_tools` roster to exclude github copilot and write the remaining
tools' rows into the same tables.

Deletes here scope by `ai_tool_name IN (cursor, claude code)` (computed
from entities.yaml at runtime) so direct/'s copilot rows are never
touched. This means you can:
- Run direct/ alone → copilot dashboards work
- Run direct/ then ai_compare/ → both work
- Delete ai_compare/ → copilot dashboards still work, comparison empties
- Delete direct/ → catches everything for the org (org-wide wipe)

## Dependencies

Run **after** `notebooks/direct/insert.py` and `notebooks/dora/insert.py`:
- `direct/insert.py` populates the github copilot rows in the two shared
  tables — this batch needs those to already exist for the comparison
  dashboard's copilot column to render.
- `dora/insert.py` creates the Acme `filter_group_id` we attach our
  `tool_type` rows to.

`insert.py` raises if the Acme filter group isn't found.

## Run

```python
exec(open("/tmp/seed-data/notebooks/ai_compare/insert.py").read())
exec(open("/tmp/seed-data/notebooks/ai_compare/delete.py").read())
```

## Known limitations

- **Scoreboard "security" column reads 0.** The scoreboard joins
  `commits_prs.merge_commit_sha = pipeline_activities.pipeline_commit_sha`,
  then to `asp_sonar_measures` / `asp_sonar_issues`. The existing dora and
  sonar generators don't share SHAs with the new `commits_prs` generator,
  so the join is empty. Fix would require either: (a) regenerating
  `pipeline_activities` with matching SHAs, or (b) regenerating
  `commits_prs` to use existing pipeline SHAs as `merge_commit_sha`.
- **Scoreboard "quality" relies on overlap with ITSM bugs.**
  `commits_prs.commit_tickets` references `ACME-N` keys from
  `transform_stage.mt_itsm_issues_hist`. If `itsm_issues.py` generated
  fewer than ~200 issues, some commit_tickets won't match — that's fine,
  the LEFT JOIN drops the unmatched rows and quality counts the rest.
- **Radar chart references hardcoded DORA KPI UUIDs.** Some radar metrics
  pull deployment frequency from existing dora filter rows, which still
  point to `https://github.com/demo-acme/project_001.git`. The radar
  chart's DORA columns will reflect existing dora data; the AI-specific
  columns come from this batch.

## Delete-order safety

`delete.py` predicates are tightly scoped to demo identifiers
(`access_level_name`, `level_name`, `level_type_name`, `org_name`, or
`created_by = 'seed-data-ai-compare@demo.io'`). Re-running delete is safe.
