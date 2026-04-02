from __future__ import annotations

import re
from typing import Literal, TYPE_CHECKING

from pipeline.constants import DOMAIN_STEM_MIN_LENGTH

if TYPE_CHECKING:
    from pipeline.models import InputRecord

LEGAL_SUFFIXES = re.compile(
    r"\b(llc|l\.l\.c|inc|incorporated|corp|corporation|ltd|limited|"
    r"lp|l\.p|llp|l\.l\.p|co|company|pllc|p\.l\.l\.c|"
    r"associates|association|assoc|group|holdings|enterprises|"
    r"services|solutions|partners|partnership|trust|foundation|"
    r"fund|ventures|capital|management|consulting|advisors|"
    r"international|global|national|usa|us|of)\b\.?",
    re.IGNORECASE,
)

NAME_SUFFIXES = re.compile(
    r",?\s*\b(jr|sr|ii|iii|iv|v|esq|cpa|md|phd|dds|do)\b\.?",
    re.IGNORECASE,
)

GEOGRAPHIC_TERMS = {
    "nevada", "california", "texas", "florida", "new york", "delaware",
    "arizona", "colorado", "georgia", "illinois", "maryland", "michigan",
    "minnesota", "north carolina", "ohio", "oregon", "pennsylvania",
    "tennessee", "utah", "virginia", "washington", "wisconsin",
    "reno", "vegas", "angeles", "francisco", "diego", "jose",
    "sacramento", "austin", "houston", "dallas", "miami", "atlanta",
    "chicago", "denver", "portland", "seattle", "phoenix",
}


def parse_name(full_name: str) -> tuple[str, str, str]:
    """Parse a full name into (first, middle, last).

    Handles:
    - "LAST, FIRST MIDDLE" comma format
    - "FIRST MIDDLE LAST" standard format
    - Single-word names -> ("", "", name)
    - Strips Jr/Sr/III/IV suffixes
    """
    name = NAME_SUFFIXES.sub("", full_name).strip()
    name = re.sub(r"\s+", " ", name).strip()

    if not name:
        return ("", "", "")

    if "," in name:
        parts = name.split(",", 1)
        last = parts[0].strip()
        rest = parts[1].strip().split()
        first = rest[0] if rest else ""
        middle = " ".join(rest[1:]) if len(rest) > 1 else ""
        return (first.lower(), middle.lower(), last.lower())

    parts = name.split()

    if len(parts) == 1:
        return ("", "", parts[0].lower())

    if len(parts) == 2:
        return (parts[0].lower(), "", parts[1].lower())

    return (parts[0].lower(), " ".join(parts[1:-1]).lower(), parts[-1].lower())


def normalize_business_name(name: str) -> str:
    """Lowercase, strip legal suffixes, punctuation, and collapse whitespace."""
    result = name.lower()
    result = LEGAL_SUFFIXES.sub("", result)
    result = re.sub(r"[^\w\s-]", "", result)
    result = re.sub(r"\s+", " ", result).strip()
    result = re.sub(r"^(the|a|an)\s+", "", result)
    return result


def generate_domain_stems(business_name: str) -> list[str]:
    """Generate candidate domain stems from a business name.

    Returns stems without TLD (e.g. ["acmecorp", "acme-corp", "acme"]).
    """
    normalized = normalize_business_name(business_name)

    if not normalized:
        return []

    # Filter out geographic terms that shouldn't form domains
    words = [w for w in normalized.split() if w.lower() not in GEOGRAPHIC_TERMS]
    if not words:
        words = normalized.split()

    stems: list[str] = []

    # Joined: "acme corp" -> "acmecorp"
    joined = "".join(words)
    if joined:
        stems.append(joined)

    # Hyphenated: "acme corp" -> "acme-corp"
    if len(words) > 1:
        stems.append("-".join(words))

    # Initials for 3+ words: "abc"
    if len(words) >= 3:
        initials = "".join(w[0] for w in words if w)
        if len(initials) >= 2:
            stems.append(initials)

    # Deduplicate while preserving order; filter invalid hostnames
    seen: set[str] = set()
    unique: list[str] = []
    for s in stems:
        s = re.sub(r"[^a-z0-9-]", "", s)
        s = s.strip("-")                          # hyphens at edges are invalid in hostnames
        if len(s) < DOMAIN_STEM_MIN_LENGTH:       # single chars are not real domains
            continue
        if s not in seen:
            seen.add(s)
            unique.append(s)

    return unique


def assign_email_strategy(record: InputRecord) -> Literal["with", "without"]:
    """Determine whether a record should use person-based or org-based email strategy."""
    # Registered agents are not business principals — search by business name only
    if record.position_type.lower() in ("agent", "registered agent"):
        return "without"

    if is_org_agent(record):
        return "without"

    first, _, last = parse_name(record.agent_name)

    # If we got at least first+last, treat as person
    if first and last:
        # Check if the name itself looks like a business
        agent_lower = record.agent_name.lower()
        if LEGAL_SUFFIXES.search(agent_lower):
            return "without"
        return "with"

    return "without"


def is_org_agent(record: InputRecord) -> bool:
    """True when the agent is an organization, not an individual."""
    if record.name_entity_type.lower() == "organization":
        agent_norm = normalize_business_name(record.agent_name)
        biz_norm = normalize_business_name(record.business_name)
        if agent_norm == biz_norm or not agent_norm:
            return True
    return False
