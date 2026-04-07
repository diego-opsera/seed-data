"""
Generator for transform_stage.mt_itsm_issues_current.

Produces Jira-style issues (stories, bugs, tasks) with sprint data for
demo-acme-direct. Scoped by customer_id = 'demo-acme-direct' for safe delete.

Sprints: 2-week sprints starting from story start_date.
Issue distribution: ~60% story, ~25% bug, ~15% task.
Status distribution: ~55% done, ~25% in progress, ~20% to do.
"""
import random
from datetime import date, timedelta
from .utils import date_range, expand_users, _sql_val

TABLE  = "mt_itsm_issues_current"
SCHEMA = "transform_stage"

INSERT_SQL = """\
INSERT INTO {catalog}.transform_stage.mt_itsm_issues_current
  (itsm_source, instance_type, data_source,
   issue_key, issue_link, issue_id, issue_type,
   parent_issue_key, parent_issue_id, parent_issue_type,
   issue_summary, issue_description, issue_priority, issue_severity,
   issue_project, issue_project_key,
   issue_resolution_name, issue_created, issue_updated, issue_resolution_date,
   issue_status, issue_changelog_itemsfield, timestamp,
   assignee_name, assignee_email, customer_id,
   record_inserted_by,
   issue_created_date, issue_updated_date,
   fix_version, sprint_id, sprint_name, sprint_state,
   sprint_start_date, sprint_end_date, sprint_complete_date, sprint_activated_date,
   sprint_goal, story_points,
   linked_issues, team_name, service_component,
   incident_start_time, incident_end_time, opsera_team_name,
   board_info, filter_info, labels, components, investment_category, service,
   source_record_insert_datetime, record_insert_datetime, source_record_insert_date)
VALUES
{values};"""

_ISSUE_TYPES = ["story"] * 6 + ["bug"] * 2 + ["task"] * 2
_PRIORITIES  = ["high"] * 2 + ["medium"] * 5 + ["low"] * 3
_TEAMS       = ["demo-backend", "demo-frontend"]

_STORY_SUMMARIES = [
    "Implement Copilot suggestion telemetry endpoint",
    "Refactor authentication middleware to support OAuth2",
    "Add dark mode support to the developer dashboard",
    "Migrate legacy REST endpoints to GraphQL",
    "Improve test coverage for the billing module",
    "Integrate GitHub Copilot metrics into the analytics pipeline",
    "Build onboarding wizard for new Copilot users",
    "Optimize database query performance for reporting views",
    "Add multi-org support to the settings panel",
    "Create automated regression tests for PR review flow",
    "Implement seat allocation API for Copilot admin",
    "Design sprint velocity tracking component",
    "Upgrade Node.js to LTS 22 across all services",
    "Add rate limiting to the public API gateway",
    "Implement SSO integration with Okta",
    "Build CSV export for Copilot usage reports",
    "Improve error messages in the CLI tool",
    "Add Copilot acceptance rate trend chart",
    "Refactor commit ingestion pipeline for reliability",
    "Implement webhook handler for GitHub App events",
]

_BUG_SUMMARIES = [
    "Fix null pointer exception in license allocation service",
    "Resolve date range filter not applying correctly in usage view",
    "Fix incorrect active user count on the IDE org-level chart",
    "Correct stale cache serving old data after re-seed",
    "Fix org isolation in activity charts for playground mode",
    "Resolve broken pagination on the PR metrics table",
    "Fix sprint velocity showing N/A after ETL refresh",
    "Correct monthly savings calculation rounding error",
    "Fix user avatar not loading in the developer profile",
    "Resolve Copilot acceptance rate showing 0% for new orgs",
]

_TASK_SUMMARIES = [
    "Update Copilot license overview SQL template variable",
    "Document the consumption layer ETL pipeline",
    "Set up monitoring alerts for the billing service",
    "Clean up deprecated API endpoints from v1",
    "Review and update ITSM data retention policy",
    "Create runbook for playground data re-seed procedure",
]


def _sprint_windows(start: date, end: date):
    """Yield (sprint_id, sprint_name, sprint_start, sprint_end) for 2-week sprints."""
    sprint_num = 1
    # align to nearest Monday
    cursor = start
    while cursor.weekday() != 0:
        cursor += timedelta(days=1)

    while cursor < end:
        sprint_end = cursor + timedelta(days=13)
        if sprint_end > end:
            sprint_end = end
        state = "closed" if sprint_end < date.today() else "active"
        yield (sprint_num, f"Sprint {sprint_num}", state, cursor, sprint_end)
        cursor = sprint_end + timedelta(days=1)
        sprint_num += 1


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name  = entities["orgs"][0]["name"]   # demo-acme-direct
    all_users = expand_users(entities, story)
    start     = date.fromisoformat(story["start_date"])
    end       = date.fromisoformat(story["end_date"])

    sprints = list(_sprint_windows(start, end))

    rng = random.Random(42)
    value_lines = []
    issue_counter = 1

    # Build epics (one per sprint for parent linking)
    epic_keys = []
    for sprint_id, sprint_name, sprint_state, sprint_start, sprint_end in sprints:
        epic_key = f"ACME-{issue_counter}"
        epic_keys.append(epic_key)
        issue_counter += 1

    # Generate ~8-12 issues per sprint
    for idx, (sprint_id, sprint_name, sprint_state, sprint_start, sprint_end) in enumerate(sprints):
        epic_key = epic_keys[idx]
        epic_id  = str(8800000 + idx)

        n_issues = rng.randint(8, 12)
        for _ in range(n_issues):
            issue_key = f"ACME-{issue_counter}"
            issue_id  = str(8800100 + issue_counter)
            issue_counter += 1

            issue_type = rng.choices(_ISSUE_TYPES)[0]
            priority   = rng.choices(_PRIORITIES)[0]
            team_name  = rng.choice(_TEAMS)

            if issue_type == "story":
                summary = rng.choice(_STORY_SUMMARIES)
                story_points = str(rng.choice([1, 2, 3, 5, 8, 13]))
            elif issue_type == "bug":
                summary = rng.choice(_BUG_SUMMARIES)
                story_points = str(rng.choice([1, 2, 3, 5]))
            else:
                summary = rng.choice(_TASK_SUMMARIES)
                story_points = str(rng.choice([1, 2, 3]))

            # Status: closed sprints mostly done; active sprint mixed
            if sprint_state == "closed":
                status = rng.choices(
                    ["done", "in progress", "to do"], weights=[80, 15, 5]
                )[0]
            else:
                status = rng.choices(
                    ["done", "in progress", "to do"], weights=[30, 40, 30]
                )[0]

            resolution_name = "Done" if status == "done" else None
            created_offset = rng.randint(0, 3)
            created_dt = sprint_start + timedelta(days=created_offset)
            if status == "done":
                updated_dt  = sprint_end - timedelta(days=rng.randint(0, 2))
                resolved_dt = updated_dt
            else:
                updated_dt  = sprint_end
                resolved_dt = None

            user = rng.choice(all_users)
            assignee_name  = user["login"]
            assignee_email = f"{user['login']}@demo-acme-direct.com"

            created_ts  = f"{created_dt.isoformat()} 09:00:00"
            updated_ts  = f"{updated_dt.isoformat()} 17:00:00"
            resolved_ts = f"{resolved_dt.isoformat()} 17:00:00" if resolved_dt else None
            sprint_start_ts = f"{sprint_start.isoformat()} 00:00:00"
            sprint_end_ts   = f"{sprint_end.isoformat()} 23:59:59"
            sprint_complete_ts = sprint_end_ts if sprint_state == "closed" else None
            insert_ts = "2026-03-31 00:00:00"

            issue_link = f"https://demo-acme-direct.atlassian.net/browse/{issue_key}"

            def ts(v):
                return f"TIMESTAMP '{v}'" if v else "NULL"

            value_lines.append(
                f"  ('jira', 'cloud', 'rest_api_pull', "
                f"{_sql_val(issue_key)}, {_sql_val(issue_link)}, {_sql_val(issue_id)}, "
                f"{_sql_val(issue_type)}, "
                f"{_sql_val(epic_key)}, {_sql_val(epic_id)}, 'epic', "
                f"{_sql_val(summary)}, NULL, "
                f"{_sql_val(priority)}, NULL, "
                f"'ACME', 'ACME', "
                f"{_sql_val(resolution_name)}, "
                f"{ts(created_ts)}, {ts(updated_ts)}, {ts(resolved_ts)}, "
                f"{_sql_val(status)}, 'status', "
                f"{ts(updated_ts)}, "
                f"{_sql_val(assignee_name)}, {_sql_val(assignee_email)}, "
                f"{_sql_val(org_name)}, "
                f"'seed-data', "
                f"DATE '{created_dt.isoformat()}', DATE '{updated_dt.isoformat()}', "
                f"ARRAY(), "  # fix_version
                f"{sprint_id}, {_sql_val(sprint_name)}, {_sql_val(sprint_state)}, "
                f"{ts(sprint_start_ts)}, {ts(sprint_end_ts)}, "
                f"{ts(sprint_complete_ts)}, {ts(sprint_start_ts)}, "
                f"NULL, "    # sprint_goal
                f"{_sql_val(story_points)}, "
                f"ARRAY(), "  # linked_issues
                f"{_sql_val(team_name)}, NULL, "  # team_name, service_component
                f"NULL, NULL, "  # incident_start/end
                f"{_sql_val(team_name)}, "  # opsera_team_name
                f"ARRAY(NAMED_STRUCT('board_id', CAST(1 AS BIGINT), 'board_name', 'ACME Board', 'board_type', 'scrum')), "  # board_info
                f"ARRAY(), ARRAY(), ARRAY(), ARRAY(), "  # filter_info, labels, components, investment_category
                f"'jira', "
                f"TIMESTAMP '{insert_ts}', TIMESTAMP '{insert_ts}', DATE '2026-03-31')"
            )

    batch_size = 500
    sqls = []
    for i in range(0, len(value_lines), batch_size):
        batch = value_lines[i:i + batch_size]
        sql = INSERT_SQL.format(catalog=catalog, values=",\n".join(batch))
        sqls.append(sql)
        # CTFC chart reads from mt_itsm_issues_hist, not _current — mirror every batch
        sqls.append(sql.replace("mt_itsm_issues_current", "mt_itsm_issues_hist"))
    return sqls
