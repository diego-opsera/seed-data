# Phase 0 Findings

## Table row counts

| Table | Rows | Earliest | Latest |
|---|---|---|---|
| enterprise_level_copilot_metrics | 166 | 2025-08-15 | 2026-01-29 |
| enterprise_user_level_copilot_metrics | 295,241 | 2025-08-15 | 2026-01-29 |
| enterprise_user_feature_level_copilot_metrics | 656,272 | 2025-08-15 | 2026-01-29 |
| enterprise_user_ide_level_copilot_metrics | 313,944 | 2025-08-15 | 2026-01-29 |
| enterprise_user_language_model_level_copilot_metrics | 958,377 | 2025-08-15 | 2026-01-29 |
| org_level_copilot_metrics | 234 | 2025-12-25 | 2026-02-01 |
| org_user_level_copilot_metrics | 212,661 | 2025-12-25 | 2026-03-14 |
| github_copilot_metrics_ide_org_level_new | 39,305 | 2025-12-25 | 2026-03-14 |
| github_copilot_metrics_ide_teams_level_new | 363,273 | 2025-12-25 | 2026-03-14 |
| github_copilot_metrics_dotcom_org_level | 9,373 | 2024-11-01 | 2026-03-15 |
| github_copilot_metrics_dotcom_teams_level | 118,093 | 2024-11-01 | 2026-03-15 |

## Existing enterprises

| enterprise_id | enterprise |
|---|---|
| 12030 | Honeywell, Inc |

## Existing orgs

| organization_id | org_name |
|---|---|
| 147471882 | HON-HCE |
| 151606513 | HON-BA |
| 151606551 | HON-ESS |
| 151606619 | HON-IA |
| 151606668 | HON-AT |
| 151606839 | HON-CORP |

**Safe fake IDs:**
- `enterprise_id`: `999999` (real IDs are ~12030)
- `organization_id`: `9990001` (real IDs are ~147M-151M)

## IDE enum values (confirmed)

```
eclipse
intellij
visualstudio
vscode
xcode
```

## Feature enum values (confirmed)

```
agent_edit
chat_inline
chat_panel_agent_mode
chat_panel_ask_mode
chat_panel_custom_mode
chat_panel_edit_mode
chat_panel_unknown_mode
code_completion
```

## Language field

The `language` field in `enterprise_user_language_model_level_copilot_metrics` is **not a clean enum** — it contains raw IDE context: code snippets, log messages, JSON blobs, etc. For demo data we use clean standard names: `python`, `typescript`, `javascript`, `java`, `go`.

## Model enum values (from real data)

Clean models seen in production:
```
gpt-4o
gpt-4o-mini
claude-3.7-sonnet
claude-4.0-sonnet
claude-4.5-sonnet
o3-mini
auto
unknown
```

## Nested array structure (confirmed from sample row)

Key finding: **`loc_added_sum`, `loc_deleted_sum`, `loc_suggested_to_add_sum`, `loc_suggested_to_delete_sum` are NULL inside all nested arrays in real data.** Generators must match this.

`totals_by_ide` entry shape:
```json
{
  "accepted_loc_sum": 0,
  "code_acceptance_activity_count": 0,
  "code_generation_activity_count": 0,
  "generated_loc_sum": 0,
  "ide": "vscode",
  "last_known_plugin_version": {
    "plugin": "copilot-chat",
    "plugin_version": "0.28.5",
    "sampled_at": "2025-08-15T03:28:36.821Z"
  },
  "user_initiated_interaction_count": 1,
  "last_known_ide_version": null,
  "loc_added_sum": null,
  "loc_deleted_sum": null,
  "loc_suggested_to_add_sum": null,
  "loc_suggested_to_delete_sum": null
}
```

Plugin name for vscode is `"copilot-chat"` (not `"copilot"`).

## Append-only / delete support

`DESCRIBE DETAIL` on `enterprise_level_copilot_metrics`:
- `properties`: `{"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true"}` — no appendOnly enforcement
- `features`: `["appendOnly","deletionVectors","invariants"]` — supported but not enforced

**Conclusion: deletes are allowed.** Cleanup = `DELETE FROM <table> WHERE enterprise_id = 999999`.
