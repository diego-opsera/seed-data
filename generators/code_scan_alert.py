"""
Generator for base_datasets.code_scan_alert.
Produces GitHub Advanced Security (GHAS) Code Scanning alerts.
Drives the "Reliability Impact: Security" chart (GHAS Code Scanning series).

Generates ~4 new alerts per weekday-spread across the story range.
Mix of open/fixed/dismissed states with realistic resolution windows.
"""
import random
from datetime import date, timedelta
from .utils import date_range, _sql_val

TABLE = "code_scan_alert"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.code_scan_alert
  (number, repository_html_url, repository_name, created_at,
   rule_severity, html_url, organization, state,
   fixed_at, dismissed_at, teams)
VALUES
{values};"""

_REPOS = [
    ("demo-acme-corp/backend",     "https://github.com/demo-acme-corp/backend"),
    ("demo-acme-corp/frontend",    "https://github.com/demo-acme-corp/frontend"),
    ("demo-acme-corp/api-gateway", "https://github.com/demo-acme-corp/api-gateway"),
]

# Weighted toward medium/high — realistic GHAS distribution
_SEVERITIES = ["critical", "high", "high", "medium", "medium", "medium", "low", "warning", "note"]

_TEAMS = [["demo-backend"], ["demo-frontend"], ["demo-backend", "demo-frontend"]]


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    alert_counter = 10001
    value_lines = []

    for d in date_range(story["start_date"], story["end_date"]):
        if d.weekday() >= 5:
            continue

        day_rng = random.Random(hash((str(d), "code_scan_count")) % (2**31))
        # ~0.8 alerts per weekday = ~4/week
        n_alerts = day_rng.choices([0, 1, 2], weights=[40, 45, 15])[0]

        for seq in range(n_alerts):
            rng = random.Random(hash((str(d), seq, "code_scan")) % (2**31))

            repo_name, repo_url = rng.choice(_REPOS)
            severity = rng.choice(_SEVERITIES)
            teams = rng.choice(_TEAMS)

            number = alert_counter
            alert_counter += 1
            html_url = f"https://github.com/{repo_name}/security/code-scanning/{number}"

            created_hour = rng.randint(9, 17)
            created_ts = f"{d.isoformat()} {created_hour:02d}:00:00"

            # 65% fixed, 15% dismissed, 20% open
            state_roll = rng.random()
            state = "fixed" if state_roll < 0.65 else ("dismissed" if state_roll < 0.80 else "open")

            fixed_at = None
            dismissed_at = None

            if state == "fixed":
                fix_date = d + timedelta(days=rng.randint(3, 30))
                if fix_date <= end:
                    fixed_at = f"{fix_date.isoformat()} {rng.randint(9, 17):02d}:00:00"
                else:
                    state = "open"
            elif state == "dismissed":
                dismiss_date = d + timedelta(days=rng.randint(7, 45))
                if dismiss_date <= end:
                    dismissed_at = f"{dismiss_date.isoformat()} {rng.randint(9, 17):02d}:00:00"
                else:
                    state = "open"

            teams_sql = "ARRAY(" + ", ".join(f"'{t}'" for t in teams) + ")"
            fixed_at_sql = f"TIMESTAMP '{fixed_at}'" if fixed_at else "NULL"
            dismissed_at_sql = f"TIMESTAMP '{dismissed_at}'" if dismissed_at else "NULL"

            value_lines.append(
                f"  ({number}, {_sql_val(repo_url)}, {_sql_val(repo_name)}, "
                f"TIMESTAMP '{created_ts}', "
                f"{_sql_val(severity)}, {_sql_val(html_url)}, {_sql_val(org_name)}, "
                f"{_sql_val(state)}, "
                f"{fixed_at_sql}, {dismissed_at_sql}, "
                f"{teams_sql})"
            )

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
