from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


class PipelineHaltError(Exception):
    """Raised when a non-recoverable error requires stopping the entire pipeline.

    Used for 401/402 API responses where retrying individual records is pointless.
    """


@dataclass
class InputRecord:
    unique_id: str
    business_name: str
    agent_name: str
    state: str
    jurisdiction: str = ""
    position_type: str = ""
    name_entity_type: str = ""
    email_biz: str = ""
    email_agent: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> InputRecord:
        return cls(
            unique_id=str(d.get("unique_id", "")),
            business_name=str(d.get("business_name", "")),
            agent_name=str(d.get("agent_name", "")),
            state=str(d.get("state", "")),
            jurisdiction=str(d.get("jurisdiction", "")),
            position_type=str(d.get("position_type", "")),
            name_entity_type=str(d.get("name_entity_type", "")),
            email_biz=str(d.get("email_biz", "") or ""),
            email_agent=str(d.get("email_agent", "") or ""),
        )


@dataclass
class EnrichmentResult:
    candidate_emails: list[str] = field(default_factory=list)
    subdomain_emails: list[str] = field(default_factory=list)
    candidate_domain: str | None = None
    source: Literal["serper", "brave"] = "serper"
    query_used: str = ""
    raw_snippets: list[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    email: str
    verdict: Literal["valid", "invalid", "unknown", "disposable"]
    score: float
    is_disposable: bool
    raw_status: str
    http_status: int
