"""
Generator for source_to_stage.survey_details_with_responses.
Simulates monthly SPACE (Satisfaction, Performance, Activity, Communication, Efficiency)
developer experience survey data for demo-acme-direct.

Story arc:
  - Monthly survey rounds over the full date range
  - ~80% of active users respond each month (min 3, max all)
  - SPACE scores trend upward: overall ~55% → ~75% over the year
  - Each dimension improves at slightly different rates

Deletion scoped via survey_id LIKE 'demo-seed-space-%'.
Filtered in SQL via: WHERE level_name = 'level_3'
                       AND arrays_overlap(level_value, array('demo-acme-corp'))
"""
import random
from datetime import date
from calendar import monthrange
from .utils import expand_users, lerp, _sql_val

TABLE  = "survey_details_with_responses"
SCHEMA = "source_to_stage"

INSERT_SQL = """\
INSERT INTO {catalog}.source_to_stage.survey_details_with_responses
  (survey_id, survey_name, description, filters,
   form_id, question_id, question,
   answer_value, responseId, lastSubmittedTime)
VALUES
{values};"""

# Hardcoded question IDs (must match space_overview.sql and siblings)
_QUESTIONS = [
    # Dimension S – Satisfaction
    ("257bb6de", "How satisfied are you with your day-to-day development tools?"),
    ("09ebec35", "How satisfied are you with the support you receive from your team?"),
    # Dimension P – Performance
    ("036cc641", "How would you rate your ability to deliver features on time?"),
    ("68215ab9", "How would you rate the quality of your code reviews?"),
    ("3914480f", "How effective is your current CI/CD pipeline?"),
    # Dimension A – Activity
    ("14d4f094", "How productive do you feel in your daily work?"),
    ("12366098", "How often are you able to complete your planned tasks each sprint?"),
    # Dimension C – Communication/Collaboration
    ("54e3ea5f", "How effective is collaboration within your team?"),
    ("3755645e", "How clear are requirements when you start a new task?"),
    # Dimension E – Efficiency
    ("04a3d0c5", "How often does technical debt slow down your work?"),
    ("300e51ec", "How well does your team manage interruptions and context switching?"),
]

# Starting average answer (1-5 scale) per dimension at t=0 and t=1
# Score formula: (answer - 1) * 25  →  answer 3.2 ≈ 55%, answer 4.0 ≈ 75%
_DIM_START = {"s": 3.0, "p": 3.3, "a": 3.1, "c": 3.2, "e": 2.9}
_DIM_END   = {"s": 4.1, "p": 4.0, "a": 3.9, "c": 4.2, "e": 3.8}

_DIM_MAP = {
    "257bb6de": "s", "09ebec35": "s",
    "036cc641": "p", "68215ab9": "p", "3914480f": "p",
    "14d4f094": "a", "12366098": "a",
    "54e3ea5f": "c", "3755645e": "c",
    "04a3d0c5": "e", "300e51ec": "e",
}

_FILTERS_SQL = (
    "NAMED_STRUCT("
    "'level_1', ARRAY('Acme Corp'), "
    "'level_2', NULL, "
    "'level_3', ARRAY('demo-acme-corp'), "
    "'level_4', NULL, "
    "'level_5', NULL, "
    "'svp', NULL, "
    "'vp', NULL, "
    "'director', NULL, "
    "'supervisor', NULL"
    ")"
)

FORM_ID = "form-space-acme-001"


def _survey_months(start: date, end: date):
    """Yield the last business day of each calendar month within [start, end]."""
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        _, last_dom = monthrange(y, m)
        d = date(y, m, last_dom)
        # Walk back to Friday if month ends on weekend
        while d.weekday() >= 5:
            d = date(d.year, d.month, d.day - 1)
        if start <= d <= end:
            yield d
        m += 1
        if m > 12:
            m = 1
            y += 1


# March 2026 incident dip: ~15-point score drop (0.6 answer-unit penalty)
# Incident hit mid-March; end-of-month survey captures the negative sentiment
_INCIDENT_SURVEY_MONTH = "2026-03"
_INCIDENT_ANSWER_PENALTY = 0.6


def _answer(dim: str, t: float, rng: random.Random, incident_month: bool = False) -> int:
    """Draw a 1–5 integer answer centred around the trending target for dimension dim."""
    target = lerp(_DIM_START[dim], _DIM_END[dim], t)
    if incident_month:
        target = max(1.0, target - _INCIDENT_ANSWER_PENALTY)
    # Gaussian noise ±0.7, then clamp and round
    raw = target + rng.gauss(0, 0.7)
    return max(1, min(5, round(raw)))


def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    all_users  = expand_users(entities, story)
    start = date.fromisoformat(story["start_date"])
    end   = date.fromisoformat(story["end_date"])
    total_days = max((end - start).days, 1)

    value_lines = []

    for survey_date in _survey_months(start, end):
        t = max(0.0, min(1.0, (survey_date - start).days / total_days))
        ym = survey_date.strftime("%Y-%m")
        survey_id   = f"demo-seed-space-{ym}"
        survey_name = f"SPACE Developer Survey - {survey_date.strftime('%B %Y')}"
        description = "Monthly developer experience survey"

        # ~80% of users respond; always at least 3
        n_respondents = max(3, round(len(all_users) * 0.80))
        respondents   = all_users[:n_respondents]

        for idx, user in enumerate(respondents):
            # Fixed response IDs 'resp-001'..'resp-005' so COUNT(DISTINCT response_id)
            # stays at 5 across all survey rounds — keeps response_rate_percentage sane
            # (formula in SQL: COUNT(DISTINCT response_id) * 100 / 5 = 100%)
            response_id = f"resp-{(idx % 5) + 1:03d}"
            # Submission time: random hour on the survey date
            u_rng = random.Random(hash((ym, user["id"], "space")) % (2**31))
            submit_hour = u_rng.randint(9, 18)
            submit_min  = u_rng.randint(0, 59)
            submit_ts   = f"TIMESTAMP '{survey_date.isoformat()} {submit_hour:02d}:{submit_min:02d}:00'"

            is_incident_month = (ym == _INCIDENT_SURVEY_MONTH)
            for q_id, q_text in _QUESTIONS:
                dim  = _DIM_MAP[q_id]
                q_rng = random.Random(hash((ym, user["id"], q_id)) % (2**31))
                ans  = _answer(dim, t, q_rng, incident_month=is_incident_month)

                value_lines.append(
                    f"  ({_sql_val(survey_id)}, {_sql_val(survey_name)}, {_sql_val(description)}, "
                    f"{_FILTERS_SQL}, "
                    f"{_sql_val(FORM_ID)}, {_sql_val(q_id)}, {_sql_val(q_text)}, "
                    f"{_sql_val(str(ans))}, {_sql_val(response_id)}, {submit_ts})"
                )

    chunk_size = 500
    statements = []
    for i in range(0, len(value_lines), chunk_size):
        chunk = value_lines[i:i + chunk_size]
        statements.append(INSERT_SQL.format(catalog=catalog, values=",\n".join(chunk)))
    return statements
