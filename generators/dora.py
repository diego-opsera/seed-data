"""
Generator for DORA / SDM (Software Delivery Metrics) tables.

Writes directly to 13 consumption_layer.sdm_* tables:
  sdm, sdm_daily_snapshot, sdm_weekly_snapshot,
  sdm_df, sdm_df_wkly,
  sdm_ltfc, sdm_ltfc_wkly,
  sdm_cfr, sdm_cfr_wkly,
  sdm_mttr, sdm_mttr_wkly,
  sdm_ctfc, sdm_ctfc_wkly

Scoped by level = 'demo-acme-corp' for safe DELETE:
  DELETE FROM consumption_layer.<table> WHERE level = 'demo-acme-corp'

DORA arc (April 2025 → March 2026):
  Apr–Jun 2025  low/medium  sporadic deploys, 42% failure rate, 18-day lead time
  Jul–Oct 2025  medium/high  Copilot adoption drives faster, safer deployments
  Nov 2025+     high/elite  cadence grows to 80 deploys/month, <2 day lead time
  Mar 18 2026   regression   incident (same date as SnapLogic scenario) bumps
                             failures and MTTR back to 'high' for that month

Org level + 2 team sub-levels per period:
  None           → org-level row (100% of org total)
  demo-backend   → 60% of org totals
  demo-frontend  → 40% of org totals
  Rates and averages (CFR%, avg_ltfc, avg_mttr, ctfc_value) are invariant under
  the team split — only counts scale.
"""
import random
from datetime import date, timedelta

from .utils import lerp, jitter

TABLE = "sdm_*"   # multiple tables; string used only for log output

# ── Identity ──────────────────────────────────────────────────────────────────

_ORG      = "demo-acme-corp"
_SUB_FRAC = {None: 1.0, "demo-backend": 0.60, "demo-frontend": 0.40}
_BY       = "seed-data"

# ── Calendar ──────────────────────────────────────────────────────────────────

_MONTHS = [
    (2025,  4), (2025,  5), (2025,  6),
    (2025,  7), (2025,  8), (2025,  9),
    (2025, 10), (2025, 11), (2025, 12),
    (2026,  1), (2026,  2), (2026,  3),
]
_N = len(_MONTHS) - 1

_MON = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
        7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

_D0   = date(2025, 4, 1)
_D1   = date(2026, 3, 31)
_SPAN = (_D1 - _D0).days
_INC  = date(2026, 3, 18)      # incident date — mirrors SnapLogic scenario

# ── DORA thresholds ───────────────────────────────────────────────────────────
# Based on DORA 2024 industry benchmarks.

def _df_flag(n):
    """Deployment frequency per month → DORA band."""
    return "elite" if n >= 30 else "high" if n >= 10 else "medium" if n >= 3 else "low"

def _ltfc_flag(days):
    """Lead time for changes in days → DORA band."""
    return "elite" if days <= 1 else "high" if days <= 7 else "medium" if days <= 30 else "low"

def _cfr_flag(pct):
    """Change failure rate percentage → DORA band."""
    return "elite" if pct <= 15 else "high" if pct <= 30 else "medium" if pct <= 45 else "low"

def _mttr_flag(hrs):
    """Mean time to recovery in hours → DORA band."""
    return "elite" if hrs <= 1 else "high" if hrs <= 24 else "medium" if hrs <= 168 else "low"

# ── SQL helpers ───────────────────────────────────────────────────────────────

class _Raw:
    """Wrap a pre-built SQL expression so _v() passes it through unquoted."""
    def __init__(self, s): self.s = s

_TS = _Raw("CURRENT_TIMESTAMP()")

def _v(x):
    if x is None:           return "NULL"
    if isinstance(x, _Raw): return x.s
    if isinstance(x, bool): return "TRUE" if x else "FALSE"
    if isinstance(x, str):  return f"'{x}'"
    return str(x)

def _row(*vals):
    return "  (" + ", ".join(_v(v) for v in vals) + ")"

def _batched(tpl, catalog, rows, size=400):
    out = []
    for i in range(0, len(rows), size):
        chunk = rows[i:i + size]
        out.append(tpl.format(catalog=catalog, values=",\n".join(chunk)))
    return out

# ── Arc ───────────────────────────────────────────────────────────────────────

def _mt(yr, mo):
    """t ∈ [0,1]: position of (yr,mo) in the 12-month arc."""
    return _MONTHS.index((yr, mo)) / _N

def _dt(d):
    """t ∈ [0,1]: position of date d in the arc."""
    return max(0.0, min(1.0, (d - _D0).days / _SPAN))

def _org_monthly(yr, mo):
    """Return all org-level monthly DORA metrics for (yr, mo)."""
    t = _mt(yr, mo)
    s = yr * 100 + mo

    # ── Deployment Frequency ──
    total  = jitter(round(lerp(8, 80, t)), 12, s)
    frate  = lerp(0.42, 0.04, t)
    if (yr, mo) == (2026, 3):          # March incident: ~6 extra failures lift monthly rate
        frate = 0.12
    failed  = max(0, round(total * frate))
    success = total - failed

    # ── Lead Time for Changes (average days per commit) ──
    avg_ltfc   = round(max(0.5, lerp(18.0, 1.5, t) + random.Random(s + 1).gauss(0, 0.4)), 2)
    commits    = jitter(round(lerp(80, 400, t)), 10, s + 2)
    total_ltfc = round(avg_ltfc * commits, 2)

    # ── Change Failure Rate ──
    cfr_pct = round(failed / total * 100, 2) if total else 0.0

    # ── MTTR (average hours per incident) ──
    # Starts at low (200 h ≈ 8.3 days), reaches elite (<1 h) by Feb 2026.
    # March incident bumps MTTR back to 18 h (high).
    avg_mttr = round(max(0.3, lerp(200.0, 0.5, t) + random.Random(s + 3).gauss(0, 3)), 2)
    if (yr, mo) == (2026, 3):
        avg_mttr = 18.0
    tot_inc = jitter(round(lerp(8, 1, t)), 20, s + 4)
    if (yr, mo) == (2026, 3):
        tot_inc = max(tot_inc, 4)    # main incident + cascading issues inflate March count
    res_inc = max(0, tot_inc - (0 if random.Random(s + 5).random() < 0.85 else 1))

    # ── Cycle Time for Changes (days from issue creation to deploy) ──
    ctfc_v = round(max(0.5, lerp(22.0, 3.5, t) + random.Random(s + 6).gauss(0, 0.4)), 2)
    ctfc_n = jitter(round(lerp(40, 120, t)), 10, s + 7)

    return {
        "failed_deployments":  failed,
        "success_deployments": success,
        "total_deployments":   total,
        "df_flag":             _df_flag(total),
        "ctfc_count_issues":   ctfc_n,
        "ctfc_value":          ctfc_v,
        "ctfc_flag":           _ltfc_flag(ctfc_v),
        "cfr_total_changes":   total,
        "cfr_total_failures":  failed,
        "cfr_value":           cfr_pct,
        "cfr_flag":            _cfr_flag(cfr_pct),
        "total_incidents":     tot_inc,
        "resolved_incidents":  res_inc,
        "avg_mttr":            avg_mttr,
        "mttr_flag":           _mttr_flag(avg_mttr),
        "commits_count":       commits,
        "total_ltfc":          total_ltfc,
        "average_ltfc":        avg_ltfc,
        "ltfc_flag":           _ltfc_flag(avg_ltfc),
    }

def _org_weekly(d):
    """Return org-level weekly DORA metrics for the Mon-starting week at d."""
    t = _dt(d)
    s = d.toordinal()
    inc_week = (d <= _INC <= d + timedelta(days=6))

    total  = max(0, jitter(round(lerp(8, 80, t) / 4.33), 15, s))
    if inc_week:
        total = max(total, round(lerp(8, 80, t) / 4.33 * 2))  # 2x emergency deploys that week
    frate  = lerp(0.42, 0.04, t)
    if inc_week:
        frate = 0.55    # most emergency hotfixes fail before the fix lands
    failed  = max(0, round(total * frate))
    success = total - failed

    avg_ltfc   = round(max(0.5, lerp(18.0, 1.5, t) + random.Random(s + 1).gauss(0, 0.4)), 2)
    commits    = max(0, jitter(round(lerp(80, 400, t) / 4.33), 10, s + 2))
    total_ltfc = round(avg_ltfc * commits, 2)

    cfr_pct = round(failed / total * 100, 2) if total else 0.0

    avg_mttr = round(max(0.3, lerp(200.0, 0.5, t) + random.Random(s + 3).gauss(0, 3)), 2)
    if inc_week:
        avg_mttr = 48.0     # serious incident — nearly 2 days to fully resolve
    tot_inc = 1 if (random.Random(s + 4).random() < lerp(0.50, 0.05, t)) else 0
    if inc_week:
        tot_inc = max(tot_inc, 3)   # main incident + 2 cascading issues
    res_inc = tot_inc

    ctfc_v = round(max(0.5, lerp(22.0, 3.5, t) + random.Random(s + 6).gauss(0, 0.4)), 2)
    ctfc_n = max(0, jitter(round(lerp(40, 120, t) / 4.33), 10, s + 7))

    return {
        "failed_deployments":  failed,
        "success_deployments": success,
        "total_deployments":   total,
        "df_flag":             _df_flag(total),
        "ctfc_count_issues":   ctfc_n,
        "ctfc_value":          ctfc_v,
        "ctfc_flag":           _ltfc_flag(ctfc_v),
        "cfr_total_changes":   total,
        "cfr_total_failures":  failed,
        "cfr_value":           cfr_pct,
        "cfr_flag":            _cfr_flag(cfr_pct),
        "total_incidents":     tot_inc,
        "resolved_incidents":  res_inc,
        "avg_mttr":            avg_mttr,
        "mttr_flag":           _mttr_flag(avg_mttr),
        "commits_count":       commits,
        "total_ltfc":          total_ltfc,
        "average_ltfc":        avg_ltfc,
        "ltfc_flag":           _ltfc_flag(avg_ltfc),
    }

def _org_daily(d):
    """Return org-level daily DORA metrics for date d."""
    t    = _dt(d)
    s    = d.toordinal()
    wday = (d.weekday() < 5)   # True on Mon–Fri
    is_inc = (d == _INC)

    # Deployments: concentrated on weekdays; extra activity on incident day
    base_daily = lerp(8, 80, t) / 22
    total = jitter(round(base_daily), 20, s) if wday else 0
    if is_inc:
        total = max(total, round(base_daily * 3))   # 3x emergency hotfix attempts
    frate  = lerp(0.42, 0.04, t)
    if is_inc:
        frate = 0.70    # most attempts fail before the fix lands
    failed  = max(0, round(total * frate)) if total else 0
    success = total - failed

    commits    = jitter(round(lerp(80, 400, t) / 22), 20, s + 2) if wday else 0
    avg_ltfc   = round(max(0.5, lerp(18.0, 1.5, t)), 2)
    total_ltfc = round(avg_ltfc * commits, 2)

    cfr_changes  = total
    cfr_failures = failed

    # Incidents: sparse daily probability derived from monthly rate
    inc_prob = lerp(8, 1, t) / 22
    has_inc  = (random.Random(s + 4).random() < inc_prob and wday) or is_inc
    tot_inc  = 1 if has_inc else 0
    if is_inc:
        tot_inc = 3     # main incident + 2 cascading issues all fire on March 18
    res_inc  = tot_inc
    mttr_today = round(lerp(200.0, 0.5, t), 1) if has_inc else 0
    if is_inc:
        mttr_today = 18    # 18-hour recovery for the big incident

    ctfc_n = jitter(round(lerp(40, 120, t) / 22), 20, s + 7) if wday else 0

    return {
        "failed_deployments":       failed,
        "success_deployments":      success,
        "total_deployments":        total,
        "ctfc_count_issues":        ctfc_n,
        "total_leadtime_in_days":   total_ltfc,
        "cfr_total_changes":        cfr_changes,
        "cfr_total_failures":       cfr_failures,
        "total_incidents":          tot_inc,
        "resolved_incidents":       res_inc,
        "total_mttr":               mttr_today,
        "commits_count":            commits,
        "total_ltfc":               total_ltfc,
    }

def _scale(m, frac):
    """
    Scale count fields by frac for team-level sub-rows.
    Rates and averages (CFR%, avg_ltfc, avg_mttr, ctfc_value) are invariant
    under the team split — they represent per-unit metrics.
    DF flag is recalculated from the scaled total_deployments.
    """
    INVARIANT = {
        "df_flag", "ctfc_flag", "cfr_flag", "mttr_flag", "ltfc_flag",
        "cfr_value", "avg_mttr", "average_ltfc", "ctfc_value",
        "total_leadtime_in_days",
    }
    out = {}
    for k, v in m.items():
        if k in INVARIANT:
            out[k] = v
        elif isinstance(v, int):
            out[k] = max(0, round(v * frac))
        elif isinstance(v, float):
            out[k] = round(v * frac, 2)
        else:
            out[k] = v
    if "total_deployments" in out:
        out["df_flag"] = _df_flag(out["total_deployments"])
    return out

# ── INSERT templates ──────────────────────────────────────────────────────────

_T_SDM = """\
INSERT INTO {catalog}.consumption_layer.sdm
  (level, sub_level, year_month, month_name,
   failed_deployments, success_deployments, total_deployments, df_flag,
   ctfc_count_issues, ctfc_value, ctfc_flag,
   cfr_total_changes, cfr_total_failures, cfr_value, cfr_flag,
   total_incidents, resolved_incidents, avg_mttr, mttr_flag,
   commits_count, total_ltfc, average_ltfc, ltfc_flag,
   active, record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_DAILY = """\
INSERT INTO {catalog}.consumption_layer.sdm_daily_snapshot
  (level, sub_level, date,
   failed_deployments, success_deployments, total_deployments,
   ctfc_count_issues, total_leadtime_in_days,
   cfr_total_changes, cfr_total_failures,
   total_incidents, resolved_incidents, total_mttr,
   commits_count, total_ltfc,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_WEEKLY = """\
INSERT INTO {catalog}.consumption_layer.sdm_weekly_snapshot
  (level, sub_level, year_month, week_name,
   failed_deployments, success_deployments, total_deployments, df_flag,
   ctfc_count_issues, ctfc_value, ctfc_flag,
   cfr_total_changes, cfr_total_failures, cfr_value, cfr_flag,
   total_incidents, resolved_incidents, avg_mttr, mttr_flag,
   commits_count, total_ltfc, average_ltfc, ltfc_flag,
   active, record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_DF = """\
INSERT INTO {catalog}.consumption_layer.sdm_df
  (level, sub_level, year_month, month_name,
   failed_deployments, success_deployments, total_deployments, df_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_DF_WKLY = """\
INSERT INTO {catalog}.consumption_layer.sdm_df_wkly
  (level, sub_level, year_month, week_name,
   failed_deployments, success_deployments, total_deployments, df_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_LTFC = """\
INSERT INTO {catalog}.consumption_layer.sdm_ltfc
  (level, sub_level, year_month, month_name,
   commits_count, total_ltfc, average_ltfc, ltfc_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_LTFC_WKLY = """\
INSERT INTO {catalog}.consumption_layer.sdm_ltfc_wkly
  (level, sub_level, year_month, week_name,
   commits_count, total_ltfc, average_ltfc, ltfc_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_CFR = """\
INSERT INTO {catalog}.consumption_layer.sdm_cfr
  (level, sub_level, year_month, month_name,
   cfr_total_changes, cfr_total_failures, cfr_value, cfr_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_CFR_WKLY = """\
INSERT INTO {catalog}.consumption_layer.sdm_cfr_wkly
  (level, sub_level, year_month, week_name,
   cfr_total_changes, cfr_total_failures, cfr_value, cfr_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_MTTR = """\
INSERT INTO {catalog}.consumption_layer.sdm_mttr
  (level, sub_level, year_month, month_name,
   total_incidents, resolved_incidents, avg_mttr, mttr_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_MTTR_WKLY = """\
INSERT INTO {catalog}.consumption_layer.sdm_mttr_wkly
  (level, sub_level, year_month, week_name,
   total_incidents, resolved_incidents, avg_mttr, mttr_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_CTFC = """\
INSERT INTO {catalog}.consumption_layer.sdm_ctfc
  (level, sub_level, year_month, month_name,
   ctfc_count_issues, ctfc_value, ctfc_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

_T_SDM_CTFC_WKLY = """\
INSERT INTO {catalog}.consumption_layer.sdm_ctfc_wkly
  (level, sub_level, year_month, week_name,
   ctfc_count_issues, ctfc_value, ctfc_flag,
   record_inserted_by, record_inserted_timestamp)
VALUES
{values};"""

# ── Row builders ─────────────────────────────────────────────────────────────

def _month_rows_sdm(catalog):
    rows = []
    for yr, mo in _MONTHS:
        ym    = yr * 100 + mo
        mname = f"{_MON[mo]} {yr}"
        org   = _org_monthly(yr, mo)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, mname,
                m["failed_deployments"], m["success_deployments"], m["total_deployments"], m["df_flag"],
                m["ctfc_count_issues"], m["ctfc_value"], m["ctfc_flag"],
                m["cfr_total_changes"], m["cfr_total_failures"], m["cfr_value"], m["cfr_flag"],
                m["total_incidents"], m["resolved_incidents"], m["avg_mttr"], m["mttr_flag"],
                m["commits_count"], m["total_ltfc"], m["average_ltfc"], m["ltfc_flag"],
                True, _BY, _TS,
            ))
    return _batched(_T_SDM, catalog, rows)

def _month_rows_df(catalog):
    rows = []
    for yr, mo in _MONTHS:
        ym    = yr * 100 + mo
        mname = f"{_MON[mo]} {yr}"
        org   = _org_monthly(yr, mo)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, mname,
                m["failed_deployments"], m["success_deployments"], m["total_deployments"], m["df_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_DF, catalog, rows)

def _month_rows_ltfc(catalog):
    rows = []
    for yr, mo in _MONTHS:
        ym    = yr * 100 + mo
        mname = f"{_MON[mo]} {yr}"
        org   = _org_monthly(yr, mo)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, mname,
                m["commits_count"], m["total_ltfc"], m["average_ltfc"], m["ltfc_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_LTFC, catalog, rows)

def _month_rows_cfr(catalog):
    rows = []
    for yr, mo in _MONTHS:
        ym    = yr * 100 + mo
        mname = f"{_MON[mo]} {yr}"
        org   = _org_monthly(yr, mo)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, mname,
                m["cfr_total_changes"], m["cfr_total_failures"], m["cfr_value"], m["cfr_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_CFR, catalog, rows)

def _month_rows_mttr(catalog):
    rows = []
    for yr, mo in _MONTHS:
        ym    = yr * 100 + mo
        mname = f"{_MON[mo]} {yr}"
        org   = _org_monthly(yr, mo)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, mname,
                m["total_incidents"], m["resolved_incidents"], m["avg_mttr"], m["mttr_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_MTTR, catalog, rows)

def _month_rows_ctfc(catalog):
    rows = []
    for yr, mo in _MONTHS:
        ym    = yr * 100 + mo
        mname = f"{_MON[mo]} {yr}"
        org   = _org_monthly(yr, mo)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, mname,
                m["ctfc_count_issues"], m["ctfc_value"], m["ctfc_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_CTFC, catalog, rows)

# ── Weekly helpers ────────────────────────────────────────────────────────────

def _week_iter():
    """
    Yield (monday: date, yr: int, mo: int, ym: int, week_name: str) for each
    Mon-starting week that overlaps [_D0, _D1].
    week_name format: "Apr W1 2025"
    year_month uses the month of the Monday.
    """
    d = _D0
    while d.weekday() != 0:       # advance to first Monday in April
        d += timedelta(days=1)
    week_counter = {}
    while d <= _D1:
        yr, mo = d.year, d.month
        key = (yr, mo)
        week_counter[key] = week_counter.get(key, 0) + 1
        wname = f"{_MON[mo]} W{week_counter[key]} {yr}"
        yield d, yr, mo, yr * 100 + mo, wname
        d += timedelta(days=7)

def _week_rows_weekly(catalog):
    rows = []
    for mon, yr, mo, ym, wname in _week_iter():
        org = _org_weekly(mon)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, wname,
                m["failed_deployments"], m["success_deployments"], m["total_deployments"], m["df_flag"],
                m["ctfc_count_issues"], m["ctfc_value"], m["ctfc_flag"],
                m["cfr_total_changes"], m["cfr_total_failures"], m["cfr_value"], m["cfr_flag"],
                m["total_incidents"], m["resolved_incidents"], m["avg_mttr"], m["mttr_flag"],
                m["commits_count"], m["total_ltfc"], m["average_ltfc"], m["ltfc_flag"],
                True, _BY, _TS,
            ))
    return _batched(_T_SDM_WEEKLY, catalog, rows)

def _week_rows_df(catalog):
    rows = []
    for mon, yr, mo, ym, wname in _week_iter():
        org = _org_weekly(mon)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, wname,
                m["failed_deployments"], m["success_deployments"], m["total_deployments"], m["df_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_DF_WKLY, catalog, rows)

def _week_rows_ltfc(catalog):
    rows = []
    for mon, yr, mo, ym, wname in _week_iter():
        org = _org_weekly(mon)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, wname,
                m["commits_count"], m["total_ltfc"], m["average_ltfc"], m["ltfc_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_LTFC_WKLY, catalog, rows)

def _week_rows_cfr(catalog):
    rows = []
    for mon, yr, mo, ym, wname in _week_iter():
        org = _org_weekly(mon)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, wname,
                m["cfr_total_changes"], m["cfr_total_failures"], m["cfr_value"], m["cfr_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_CFR_WKLY, catalog, rows)

def _week_rows_mttr(catalog):
    rows = []
    for mon, yr, mo, ym, wname in _week_iter():
        org = _org_weekly(mon)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, wname,
                m["total_incidents"], m["resolved_incidents"], m["avg_mttr"], m["mttr_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_MTTR_WKLY, catalog, rows)

def _week_rows_ctfc(catalog):
    rows = []
    for mon, yr, mo, ym, wname in _week_iter():
        org = _org_weekly(mon)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, ym, wname,
                m["ctfc_count_issues"], m["ctfc_value"], m["ctfc_flag"],
                _BY, _TS,
            ))
    return _batched(_T_SDM_CTFC_WKLY, catalog, rows)

# ── Daily snapshot ────────────────────────────────────────────────────────────

def _daily_rows(catalog):
    rows = []
    d = _D0
    while d <= _D1:
        org = _org_daily(d)
        for sub, frac in _SUB_FRAC.items():
            m = _scale(org, frac)
            rows.append(_row(
                _ORG, sub, _Raw(f"DATE '{d.isoformat()}'"),
                m["failed_deployments"], m["success_deployments"], m["total_deployments"],
                m["ctfc_count_issues"], m["total_leadtime_in_days"],
                m["cfr_total_changes"], m["cfr_total_failures"],
                m["total_incidents"], m["resolved_incidents"], m["total_mttr"],
                m["commits_count"], m["total_ltfc"],
                _BY, _TS,
            ))
        d += timedelta(days=1)
    return _batched(_T_SDM_DAILY, catalog, rows, size=300)

# ── Entry point ───────────────────────────────────────────────────────────────

def generate(catalog: str, entities: dict, story: dict) -> list[str]:
    stmts = []
    stmts += _month_rows_sdm(catalog)
    stmts += _month_rows_df(catalog)
    stmts += _month_rows_ltfc(catalog)
    stmts += _month_rows_cfr(catalog)
    stmts += _month_rows_mttr(catalog)
    stmts += _month_rows_ctfc(catalog)
    stmts += _week_rows_weekly(catalog)
    stmts += _week_rows_df(catalog)
    stmts += _week_rows_ltfc(catalog)
    stmts += _week_rows_cfr(catalog)
    stmts += _week_rows_mttr(catalog)
    stmts += _week_rows_ctfc(catalog)
    stmts += _daily_rows(catalog)
    return stmts
