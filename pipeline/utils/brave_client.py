from __future__ import annotations

import asyncio
import logging
import re
from typing import Literal
from urllib.parse import urlparse

import aiohttp
from fuzzywuzzy import fuzz

from pipeline.constants import SERVICE_BACKOFF
from pipeline.models import EnrichmentResult, PipelineHaltError
from pipeline.utils.backoff import with_backoff
from pipeline.utils.rate_limiter import TokenBucket

logger = logging.getLogger("pipeline.producer")

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


class BraveClient:
    def __init__(
        self,
        api_key: str,
        session: aiohttp.ClientSession,
        rate_limiter: TokenBucket,
        *,
        dry_run: bool = False,
        max_attempts: int = 3,
        jitter: float = 0.2,
    ) -> None:
        self.api_key = api_key
        self.session = session
        self.rate_limiter = rate_limiter
        self.dry_run = dry_run
        self.max_attempts = max_attempts
        self.jitter = jitter
        self._base, self._max_delay = SERVICE_BACKOFF["brave"]

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
                source="brave",
                query_used=f"[dry-run] {query}",
                raw_snippets=["[dry-run stub snippet]"],
            )

        await self.rate_limiter.acquire()

        data = await with_backoff(
            lambda: self._call_api(query),
            max_attempts=self.max_attempts,
            base_delay=self._base,
            max_delay=self._max_delay,
            jitter=self.jitter,
            retryable=_is_retryable,
            on_retry=lambda attempt, exc, delay: logger.debug(
                "Brave retry %d: %s (wait %.1fs)", attempt, exc, delay,
            ),
        )

        return self._extract(data, business_name, query, domain_hint=domain_hint, strategy=strategy, agent_name=agent_name)

    async def _call_api(self, query: str) -> dict:
        headers = {
            "X-Subscription-Token": self.api_key,
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
        }
        params = {
            "q": query,
            "count": 10,
            "country": "us",
            "search_lang": "en",
            "text_decorations": "false",
        }

        async with self.session.get(
            "https://api.search.brave.com/res/v1/web/search",
            params=params,
            headers=headers,
        ) as resp:
            if resp.status == 401:
                raise PipelineHaltError("Brave API key invalid or missing (401)")
            if resp.status in (400, 422):
                body = await resp.text()
                raise PipelineHaltError(f"Brave bad request ({resp.status}): {body}")
            if resp.status in (429, 500, 503):
                raise _RetryableHTTPError(resp.status)
            resp.raise_for_status()
            return await resp.json()

    def _extract(
        self,
        data: dict,
        business_name: str,
        query: str,
        domain_hint: str | None = None,
        strategy: str = "without",
        agent_name: str | None = None,
    ) -> EnrichmentResult:
        emails: list[str] = []
        snippets: list[str] = []
        domain: str | None = None

        web_results = data.get("web", {}).get("results", [])

        for result in web_results:
            desc = result.get("description", "")
            if desc:
                snippets.append(desc)
                emails.extend(EMAIL_RE.findall(desc))

            for extra in result.get("extra_snippets", []):
                snippets.append(extra)
                emails.extend(EMAIL_RE.findall(extra))

        seen: set[str] = set()
        unique_emails: list[str] = []
        for e in emails:
            lower = e.lower()
            if lower not in seen:
                seen.add(lower)
                unique_emails.append(lower)

        norm_biz = business_name.lower()
        first_organic_domain: str | None = None
        for result in web_results:
            url = result.get("url", "")
            if not url:
                continue
            netloc = urlparse(url).netloc.lower().lstrip("www.")
            if not netloc:
                continue
            if first_organic_domain is None:
                first_organic_domain = netloc
            netloc_base = netloc.rsplit(".", 1)[0] if "." in netloc else netloc
            netloc_norm = netloc_base.replace("-", "")
            if fuzz.ratio(norm_biz.replace(" ", ""), netloc_norm) >= 85:
                domain = netloc
                break
            long_name = result.get("profile", {}).get("long_name", "")
            if long_name:
                ln_base = long_name.lower().rsplit(".", 1)[0] if "." in long_name else long_name.lower()
                ln_norm = ln_base.replace("-", "")
                if fuzz.ratio(norm_biz.replace(" ", ""), ln_norm) >= 85:
                    domain = long_name.lower()
                    break
        # For with-strategy, fall back to first organic domain if fuzzy match found nothing
        if not domain and strategy == "with" and first_organic_domain:
            domain = first_organic_domain
            logger.debug("Brave using first organic domain as fallback: %s", domain)

        # Split emails into confirmed-domain and subdomain buckets.
        # Unrelated domains are discarded entirely.
        known_domain = domain or domain_hint
        subdomain_emails: list[str] = []
        if known_domain:
            filtered: list[str] = []
            for e in unique_emails:
                host = e.split("@")[1] if "@" in e else ""
                if e.endswith(f"@{known_domain}"):
                    filtered.append(e)
                elif host.endswith(f".{known_domain}"):
                    subdomain_emails.append(e)
            unique_emails = filtered

        # For "with" strategy, only keep snippet emails whose local part
        # fuzzy-matches the agent name. Unmatched emails are discarded —
        # the producer will generate personal patterns from the domain instead.
        if strategy == "with" and agent_name:
            parts = agent_name.strip().lower().split()
            first = parts[0] if parts else ""
            last = parts[-1] if len(parts) > 1 else ""
            name_variants = [v for v in [
                f"{first}{last}",
                f"{first}.{last}",
                f"{first}_{last}",
                f"{first[0]}{last}" if first else "",
                first,
                last,
            ] if v]
            matched: list[str] = []
            for e in unique_emails:
                local = e.split("@")[0]
                score = max(fuzz.ratio(local, v) for v in name_variants)
                if score >= 75:
                    matched.append(e)
                else:
                    logger.debug(
                        "Brave snippet email %s discarded for with-strategy (best score %d)",
                        e, score,
                    )
            unique_emails = matched

        return EnrichmentResult(
            candidate_emails=unique_emails,
            subdomain_emails=subdomain_emails,
            candidate_domain=domain,
            source="brave",
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
        return True  # only 429, 500, 503 ever raise this
    if isinstance(exc, aiohttp.ClientResponseError):
        return False  # explicit HTTP errors are not transient
    return isinstance(exc, (aiohttp.ClientConnectionError, asyncio.TimeoutError))
