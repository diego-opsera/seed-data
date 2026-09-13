"""
Port of VisualForge's developer-identity gates, so the roster can be validated
before a single row is seeded.

Source: visualforge@b8377ed22
  services/unified-backend/src/modules/integration/databricks/etl/sharedIdentity.js
  — isBot, isRealUserEmail, hasLikelyRealEmailLocalPart, isLikelyHumanLogin,
    isLikelyHashedIdentityToken, isLikelySyntheticDisplayName,
    validateDevIdentifier (plus emailToName / emailToInitials from syncDvi.js)

Both the DVI and Tokenomics ETLs run every identity through these before a
developer is created. A row that fails is dropped silently — no error, no log
line on the dashboard — so the roster is checked here instead of being
discovered missing after a seed.

Run the self-check:
    python3 -m vf.generators.identity_gates
"""
import re

# ── isBot ───────────────────────────────────────────────────────────────────
_BOT_SUBSTRINGS = ("[bot]", "dependabot", "github-actions", "renovate")


def is_bot(author) -> bool:
    a = str(author or "").strip().lower()
    if not a:
        return False
    if any(s in a for s in _BOT_SUBSTRINGS) or a == "web-flow":
        return True
    local = a[: a.index("@")] if "@" in a else a
    if (
        local in ("agent", "cursoragent", "copilot")
        or "cursoragent" in local
        or "dependabot" in local
        or local.endswith("-bot")
        or local.endswith("[bot]")
        or re.match(r"^copilot(-|$)", local)
        or re.match(r"^github-actions", local)
    ):
        return True
    if a.endswith("@opsera.dev") and ("agent" in local):
        return True
    return False


def is_real_user_email(value) -> bool:
    email = str(value or "").strip().lower()
    if not email or "@" not in email:
        return False
    return not email.endswith("@github-member.local")


def is_likely_hashed_identity_token(value) -> bool:
    v = str(value or "").strip().lower()
    if not v:
        return False
    if re.fullmatch(r"[a-f0-9]{20,}", v):
        return True
    if re.fullmatch(r"[a-z0-9]{32,}", v):
        digits = sum(c.isdigit() for c in v)
        vowels = sum(c in "aeiou" for c in v)
        if digits >= 12 and vowels <= 2:
            return True
        if not re.search(r"[._-]", v) and len(set(v)) >= 16:
            return True
    if re.fullmatch(r"[a-z0-9]{28,31}", v) and not re.search(r"[._-]", v):
        if len(set(v)) >= 16:
            return True
    return False


def has_likely_real_email_local_part(email) -> bool:
    normalized = str(email or "").strip().lower()
    at = normalized.find("@")
    if at <= 0:
        return False
    local = normalized[:at]
    if len(local) <= 12:          # relaxed for short alphanumeric employee ids
        return True
    return not is_likely_hashed_identity_token(local)


def is_likely_human_login(login) -> bool:
    v = str(login or "").strip().lower()
    if not v or is_bot(v):
        return False
    if "svc" in v or "service" in v or "admin" in v:
        return False
    if len(v) > 24 and re.fullmatch(r"[a-f0-9]+", v):
        return False
    if re.fullmatch(r"[0-9]+", v):
        return False
    return bool(re.fullmatch(r"[a-z0-9._-]{3,}", v))


def is_likely_synthetic_display_name(name) -> bool:
    raw = str(name or "").strip().lower()
    if not raw:
        return False
    if "{gitusername}" in raw:
        return True
    if raw == "bot" or "[bot]" in raw or "dependabot" in raw:
        return True
    tokens = raw.split()
    first = re.sub(r"[^a-z0-9]", "", tokens[0]) if tokens else ""
    return is_likely_hashed_identity_token(first)


def validate_dev_identifier(identifier):
    """Returns the normalized key, or None when the ETL would drop the row."""
    key = str(identifier or "").strip().lower()
    if not key or is_bot(key):
        return None
    if not is_real_user_email(key):
        return None
    if not has_likely_real_email_local_part(key):
        return None
    return key


def email_to_name(email) -> str:
    """Port of syncDvi.js emailToName — how the UI will label the developer."""
    prefix = str(email or "").split("@")[0]
    parts = [p for p in re.split(r"[._\-]", prefix) if p and re.search(r"[a-zA-Z]", p)]
    parts = [p[0].upper() + p[1:] for p in parts]
    return " ".join(parts) or re.sub(r"[._\-]", " ", prefix).strip()


def email_to_initials(email) -> str:
    name = email_to_name(email)
    parts = [p for p in name.split(" ") if p]
    if len(parts) >= 2:
        return (parts[0][0] + parts[-1][0]).upper()
    return name[:2].upper()


# ── Roster validation ───────────────────────────────────────────────────────
def check_user(user: dict) -> list[str]:
    """Return a list of gate failures for one roster entry ([] means it passes)."""
    problems = []
    login = user.get("login")
    email = user.get("email")

    if not email:
        problems.append("no email — the ETL joins every source on email")
        return problems

    if validate_dev_identifier(email) is None:
        reasons = []
        if is_bot(email):
            reasons.append("reads as a bot")
        if not is_real_user_email(email):
            reasons.append("not a real address")
        if not has_likely_real_email_local_part(email):
            reasons.append(f"local part '{email.split('@')[0]}' looks hashed")
        problems.append(f"email rejected by validateDevIdentifier ({', '.join(reasons)})")

    if not is_likely_human_login(login):
        reasons = []
        if is_bot(login):
            reasons.append("reads as a bot")
        for bad in ("svc", "service", "admin"):
            if bad in str(login).lower():
                reasons.append(f"contains '{bad}'")
        if not re.fullmatch(r"[a-z0-9._-]{3,}", str(login or "")):
            reasons.append("fails ^[a-z0-9._-]{3,}$")
        problems.append(f"login '{login}' rejected by isLikelyHumanLogin ({', '.join(reasons) or 'unknown'})")

    derived = email_to_name(email)
    if is_likely_synthetic_display_name(derived):
        problems.append(f"derived display name '{derived}' reads as synthetic")

    declared = str(user.get("name") or "").strip()
    if declared and declared != derived:
        problems.append(
            f"name drift — entities.yaml says '{declared}' but the UI will show '{derived}'"
        )
    return problems


def verify_roster(users: list[dict]) -> dict:
    """Validate a roster. Returns {'ok': bool, 'failures': {email: [problems]}}."""
    failures = {}
    for u in users:
        problems = check_user(u)
        if problems:
            failures[u.get("email") or u.get("login") or "<unknown>"] = problems

    emails = [str(u.get("email", "")).lower() for u in users]
    logins = [str(u.get("login", "")).lower() for u in users]
    for label, values in (("email", emails), ("login", logins)):
        dupes = {v for v in values if v and values.count(v) > 1}
        for d in dupes:
            failures.setdefault(d, []).append(f"duplicate {label}")

    return {"ok": not failures, "failures": failures}


if __name__ == "__main__":
    import os
    import sys
    import yaml

    here = os.path.dirname(os.path.abspath(__file__))
    cfg = os.path.join(here, "..", "config", "entities.yaml")
    with open(cfg) as f:
        entities = yaml.safe_load(f)
    users = entities["users"]

    result = verify_roster(users)
    print(f"Checked {len(users)} roster entries against VisualForge's identity gates\n")
    for u in users:
        email = u["email"]
        print(f"  {'PASS' if not check_user(u) else 'FAIL'}  "
              f"{u['login']:<16} {email:<40} "
              f"→ {email_to_name(email)} ({email_to_initials(email)})")

    if result["ok"]:
        print(f"\nAll {len(users)} entries pass. Safe to use as the seed roster.")
    else:
        print("\nFAILURES:")
        for who, problems in result["failures"].items():
            for p in problems:
                print(f"  {who}: {p}")
        sys.exit(1)
