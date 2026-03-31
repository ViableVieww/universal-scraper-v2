from __future__ import annotations

import logging
import re
from typing import Literal
from urllib.parse import urlparse

import aiohttp
from fuzzywuzzy import fuzz

from pipeline.models import EnrichmentResult, PipelineHaltError
from pipeline.utils.backoff import SERVICE_BACKOFF, with_backoff

logger = logging.getLogger("pipeline.producer")

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


class SerperClient:
    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        *,
        dry_run: bool = False,
        max_attempts: int = 3,
        jitter: float = 0.2,
    ) -> None:
        self.api_key = api_key
        self.session = session
        self.dry_run = dry_run
        self.max_attempts = max_attempts
        self.jitter = jitter
        self._base, self._max_delay = SERVICE_BACKOFF["serper"]

    async def enrich(
        self,
        business_name: str,
        agent_name: str | None,
        state: str,
        domain_hint: str | None,
        strategy: Literal["with", "without"],
    ) -> EnrichmentResult:
        query = self._build_query(business_name, agent_name, state, domain_hint, strategy)

        if self.dry_run:
            return EnrichmentResult(
                candidate_emails=["dryrun@example-business.com"],
                candidate_domain="example-business.com",
                source="serper",
                query_used=f"[dry-run] {query}",
                raw_snippets=["[dry-run stub snippet]"],
            )

        data = await with_backoff(
            lambda: self._call_api(query),
            max_attempts=self.max_attempts,
            base_delay=self._base,
            max_delay=self._max_delay,
            jitter=self.jitter,
            retryable=_is_retryable,
            on_retry=lambda attempt, exc, delay: logger.debug(
                "Serper retry %d: %s (wait %.1fs)", attempt, exc, delay,
            ),
        )

        return self._extract(data, business_name, query)

    async def _call_api(self, query: str) -> dict:
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }
        payload = {"q": query, "num": 10, "gl": "us", "hl": "en"}

        async with self.session.post(
            "https://google.serper.dev/search",
            json=payload,
            headers=headers,
        ) as resp:
            if resp.status == 401:
                raise PipelineHaltError("Serper API key invalid or missing (401)")
            if resp.status == 400:
                body = await resp.text()
                raise PipelineHaltError(f"Serper bad request (400): {body}")
            if resp.status in (429, 500, 503):
                raise _RetryableHTTPError(resp.status)
            resp.raise_for_status()
            return await resp.json()

    def _extract(self, data: dict, business_name: str, query: str) -> EnrichmentResult:
        emails: list[str] = []
        snippets: list[str] = []
        domain: str | None = None

        # Extract emails from organic snippets
        for result in data.get("organic", []):
            snippet = result.get("snippet", "")
            if snippet:
                snippets.append(snippet)
                emails.extend(EMAIL_RE.findall(snippet))

        # Deduplicate emails
        seen: set[str] = set()
        unique_emails: list[str] = []
        for e in emails:
            lower = e.lower()
            if lower not in seen:
                seen.add(lower)
                unique_emails.append(lower)

        # Extract domain from knowledgeGraph first
        kg = data.get("knowledgeGraph", {})
        if kg and kg.get("website"):
            parsed = urlparse(kg["website"])
            domain = parsed.netloc.lower().lstrip("www.")

        # Fallback: highest-ranked organic link with fuzzy match
        if not domain:
            norm_biz = business_name.lower()
            for result in data.get("organic", []):
                link = result.get("link", "")
                if not link:
                    continue
                netloc = urlparse(link).netloc.lower().lstrip("www.")
                # Strip TLD for fuzzy comparison
                netloc_base = netloc.rsplit(".", 1)[0] if "." in netloc else netloc
                if fuzz.ratio(norm_biz.replace(" ", ""), netloc_base) >= 80:
                    domain = netloc
                    break

        return EnrichmentResult(
            candidate_emails=unique_emails,
            candidate_domain=domain,
            source="serper",
            query_used=query,
            raw_snippets=snippets,
        )

    @staticmethod
    def _build_query(
        business_name: str,
        agent_name: str | None,
        state: str,
        domain_hint: str | None,
        strategy: Literal["with", "without"],
    ) -> str:
        if strategy == "with" and agent_name:
            if domain_hint:
                return f'"{agent_name}" "{business_name}" email contact site:{domain_hint}'
            return f'"{agent_name}" "{business_name}" email contact'
        else:
            if domain_hint:
                return f"site:{domain_hint} contact"
            return f'"{business_name}" contact email {state}'


class _RetryableHTTPError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"HTTP {status}")


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, _RetryableHTTPError):
        return exc.status in (429, 500, 503)
    return isinstance(exc, (aiohttp.ClientError, asyncio.TimeoutError))


import asyncio  # noqa: E402 (used in _is_retryable)
