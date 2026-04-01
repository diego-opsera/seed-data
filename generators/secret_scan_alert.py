"""
Generator for base_datasets.secret_scan_alert.
Produces GitHub Advanced Security (GHAS) Secret Scanning alerts.
Drives the "Reliability Impact: Security" chart (GHAS Secret Scanning series).

Generates ~2 new alerts per week. Secrets get resolved quickly (75% within 2 weeks).
"""
import random
from datetime import date, timedelta
from .utils import date_range, _sql_val

TABLE = "secret_scan_alert"

INSERT_SQL = """\
INSERT INTO {catalog}.base_datasets.secret_scan_alert
  (number, repository_html_url, repository_name, severity,
   html_url, organization, created_at, state, resolved_at, teams)
VALUES
{values};"""

_REPOS = [
    ("demo-acme-corp/backend",     "https://github.com/demo-acme-corp/backend"),
    ("demo-acme-corp/frontend",    "https://github.com/demo-acme-corp/frontend"),
    ("demo-acme-corp/api-gateway", "https://github.com/demo-acme-corp/api-gateway"),
]

# Secrets tend to be high/critical severity
_SEVERITIES = ["critical", "high", "high", "medium", "medium", "low"]

_TEAMS = [["demo-backend"], ["demo-frontend"], ["demo-backend", "demo-frontend"]]

# Mirrors the code_scan spikes at roughly half the volume.
_SPIKE_DAYS = {
    date(2026, 3, 5):   2,
    date(2026, 3, 6):   3,
    date(2026, 3, 7):   5,   # SEV1 peak
    date(2026, 3, 8):   4,
    date(2026, 3, 9):   3,
    date(2026, 3, 10):  1,
    date(2025, 11, 17): 1,
    date(2025, 11, 18): 3,   # secondary peak
    date(2025, 11, 19): 2,
    date(2025, 11, 20): 1,
}


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    org_name = entities["orgs"][0]["name"]
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])

    alert_counter = 5001
    value_lines = []

    for d in date_range(story["start_date"], story["end_date"]):
        if d in _SPIKE_DAYS:
            n_alerts = _SPIKE_DAYS[d]
        elif d.weekday() >= 5:
            continue
        else:
            day_rng = random.Random(hash((str(d), "secret_scan_count")) % (2**31))
            # ~0.4 alerts per weekday = ~2/week
            n_alerts = day_rng.choices([0, 1], weights=[60, 40])[0]

        for seq in range(n_alerts):
            rng = random.Random(hash((str(d), seq, "secret_scan")) % (2**31))

            repo_name, repo_url = rng.choice(_REPOS)
            severity = rng.choice(_SEVERITIES)
            teams = rng.choice(_TEAMS)

            number = alert_counter
            alert_counter += 1
            html_url = f"https://github.com/{repo_name}/security/secret-scanning/{number}"

            created_hour = rng.randint(9, 17)
            created_ts = f"{d.isoformat()} {created_hour:02d}:00:00"

            # 75% resolved (secrets get fixed fast), 25% remain open
            resolved_at = None
            if rng.random() < 0.75:
                state = "resolved"
                resolve_date = d + timedelta(days=rng.randint(1, 14))
                if resolve_date <= end:
                    resolved_at = f"{resolve_date.isoformat()} {rng.randint(9, 17):02d}:00:00"
                else:
                    state = "open"
            else:
                state = "open"

            teams_sql = "ARRAY(" + ", ".join(f"'{t}'" for t in teams) + ")"
            resolved_at_sql = f"TIMESTAMP '{resolved_at}'" if resolved_at else "NULL"

            value_lines.append(
                f"  ({number}, {_sql_val(repo_url)}, {_sql_val(repo_name)}, "
                f"{_sql_val(severity)}, {_sql_val(html_url)}, {_sql_val(org_name)}, "
                f"TIMESTAMP '{created_ts}', "
                f"{_sql_val(state)}, {resolved_at_sql}, "
                f"{teams_sql})"
            )

    return [INSERT_SQL.format(catalog=catalog, values=",\n".join(value_lines))]
