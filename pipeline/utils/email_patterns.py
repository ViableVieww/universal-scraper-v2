from __future__ import annotations

from typing import Literal

from pipeline.constants import MAX_WITHOUT_CANDIDATES


# Ranked by empirical likelihood (most common corporate patterns first)
def generate_personal_patterns(first: str, last: str, domain: str) -> list[str]:
    """Generate 13 personal email patterns ranked by likelihood.

    Args:
        first: Lowercase first name.
        last: Lowercase last name.
        domain: Bare domain (e.g. "acme.com").

    Returns:
        List of email candidates, most likely first.
    """
    if not first or not last or not domain:
        return []

    f = first[0]
    l = last[0]

    return [
        f"{first}.{last}@{domain}",
        f"{f}{last}@{domain}",
        f"{first}{last}@{domain}",
        f"{first}@{domain}",
        f"{f}.{last}@{domain}",
        f"{first}_{last}@{domain}",
        f"{first}{l}@{domain}",
        f"{last}.{first}@{domain}",
        f"{last}{first}@{domain}",
        f"{first}.{l}@{domain}",
        f"{l}{first}@{domain}",
        f"{last}@{domain}",
        f"{last}{f}@{domain}",
    ]


def generate_generic_patterns(domain: str) -> list[str]:
    """Generate 8 generic/org email patterns.

    Args:
        domain: Bare domain (e.g. "acme.com").

    Returns:
        List of generic email candidates.
    """
    if not domain:
        return []

    return [
        f"info@{domain}",
        f"contact@{domain}",
        f"hello@{domain}",
        f"office@{domain}",
        f"admin@{domain}",
        f"support@{domain}",
        f"sales@{domain}",
        f"hi@{domain}",
    ]


def generate_ranked_candidates(
    first: str,
    last: str,
    domain: str,
    strategy: Literal["with", "without"],
    max_candidates: int = 5,
) -> list[str]:
    """Generate top-N ranked email candidates based on strategy.

    For "with" (person-based): returns top max_candidates from 13 personal patterns.
    For "without" (org-based): returns top MAX_WITHOUT_CANDIDATES generic patterns.
    """
    if strategy == "with":
        patterns = generate_personal_patterns(first, last, domain)
        return patterns[:max_candidates]
    else:
        return generate_generic_patterns(domain)[:MAX_WITHOUT_CANDIDATES]
