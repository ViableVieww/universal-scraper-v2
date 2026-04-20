from __future__ import annotations

import asyncio
import json
import logging

import aiosqlite
from fuzzywuzzy import fuzz

from pipeline.config import PipelineConfig
from pipeline.constants import CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD, CONSUMER_POLL_MAX_INTERVAL_SECONDS
from pipeline.models import PipelineHaltError
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.zuhal_client import ZuhalClient
from pipeline import db

logger = logging.getLogger("pipeline.consumer")


class ConsumerWorker:
    def __init__(
        self,
        config: PipelineConfig,
        conn: aiosqlite.Connection,
        cost_tracker: CostTracker,
        zuhal: ZuhalClient,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self.config = config
        self.conn = conn
        self.cost_tracker = cost_tracker
        self.zuhal = zuhal
        self.stop_event = stop_event or asyncio.Event()
        self._sem = asyncio.Semaphore(config.zuhal_concurrency)
        self._consecutive_api_errors: int = 0

    async def run(self) -> None:
        base_interval = float(self.config.consumer_poll_interval)
        poll_interval = base_interval
        consecutive_empty = 0

        logger.info("Consumer starting (base poll interval: %.0fs)", base_interval)

        while not self.stop_event.is_set():
            rows = await db.fetch_pending_validation(self.conn, limit=10)

            if not rows:
                # Adaptive backoff: double interval after threshold consecutive empties
                consecutive_empty += 1
                if consecutive_empty >= CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD:
                    poll_interval = min(poll_interval * 2, CONSUMER_POLL_MAX_INTERVAL_SECONDS)

                # Check if producer is done
                producer_done = await db.get_checkpoint(self.conn, "producer_done")
                if producer_done == "true":
                    # Double-check: any remaining pending rows?
                    final_check = await db.fetch_pending_validation(self.conn, limit=1)
                    if not final_check:
                        if consecutive_empty >= CONSUMER_POLL_EMPTY_BACKOFF_THRESHOLD:
                            logger.info("Consumer: queue drained and producer done — exiting")
                            break
                    else:
                        # Rows appeared — reset backoff and process them
                        consecutive_empty = 0
                        poll_interval = base_interval
                        continue

                await asyncio.sleep(poll_interval)
                continue

            # Rows found — reset adaptive backoff
            consecutive_empty = 0
            poll_interval = base_interval

            # Process batch concurrently
            tasks = [self._validate_record(row) for row in rows]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, BaseException) and not isinstance(res, PipelineHaltError):
                    logger.error("Unexpected error in validation task", exc_info=res)

        logger.info("Consumer finished")

    async def _validate_record(self, row: aiosqlite.Row) -> None:
        async with self._sem:
            unique_id = row["unique_id"]
            raw_candidates = row["candidate_emails"]

            if not raw_candidates:
                logger.warning("No candidate_emails for %s — marking failed", unique_id)
                await db.update_record_status(self.conn, unique_id, "validation_failed")
                return

            try:
                candidates: list[str] = json.loads(raw_candidates)
            except (json.JSONDecodeError, TypeError):
                logger.warning("Invalid candidate_emails JSON for %s", unique_id)
                await db.update_record_status(self.conn, unique_id, "validation_failed")
                return

            # Mark as in-progress to prevent duplicate picks on the next poll
            await db.update_record_status(self.conn, unique_id, "validating")

            last_verdict: str | None = None

            for idx, email in enumerate(candidates):
                try:
                    result = await self.zuhal.validate(email)
                    self.cost_tracker.record_call("zuhal")
                except PipelineHaltError:
                    # Restore status so it can be retried after the halt is resolved
                    await db.update_record_status(self.conn, unique_id, "pending_validation")
                    raise
                except Exception as exc:
                    logger.debug(
                        "Zuhal error for %s candidate %s: %s", unique_id, email, exc,
                    )
                    await db.insert_failure(
                        self.conn,
                        unique_id,
                        "zuhal",
                        idx + 1,
                        type(exc).__name__,
                        str(exc),
                    )
                    self._consecutive_api_errors += 1
                    if self._consecutive_api_errors >= self.config.max_consecutive_errors:
                        raise PipelineHaltError(
                            f"Zuhal returning consistent errors — "
                            f"{self._consecutive_api_errors} consecutive failures. "
                            "Halting pipeline."
                        )
                    continue

                # Successful API response — reset the error streak
                self._consecutive_api_errors = 0
                last_verdict = result.verdict

                if result.verdict in ("valid", "accept-all"):
                    score = compute_confidence_score(
                        email=email,
                        candidate_domain=row["candidate_domain"],
                        strategy=row["strategy"] or "without",
                        verdict=result.verdict,
                        agent_name=row["agent_name"] or "",
                    )
                    await db.update_record_status(
                        self.conn,
                        unique_id,
                        "validated",
                        candidate_email=email,
                        zuhal_status=result.verdict,
                        zuhal_score=score,
                    )
                    logger.info("Validated: %s -> %s (%s)", unique_id, email, result.verdict)
                    return

                # unknown/invalid/disposable — move to next candidate, no retry
                logger.debug(
                    "Candidate %s for %s: %s — trying next",
                    email, unique_id, result.verdict,
                )
                continue

            # All candidates exhausted without a valid verdict
            await db.update_record_status(
                self.conn,
                unique_id,
                "validation_failed",
                validation_attempts=len(candidates),
                zuhal_status=last_verdict,
            )
            logger.debug("All candidates failed for %s (last verdict: %s)", unique_id, last_verdict)


_GENERIC_PREFIXES: frozenset[str] = frozenset({
    "info", "contact", "hello", "admin",
    "support", "sales", "help",
})


def _name_matches_email(local: str, agent_name: str) -> bool:
    """True if the email local part fuzzy-matches the agent name (≥75)."""
    parts = agent_name.strip().lower().split()
    first = parts[0] if parts else ""
    last = parts[-1] if len(parts) > 1 else ""
    variants = [v for v in [
        f"{first}{last}",
        f"{first}.{last}",
        f"{first}_{last}",
        f"{first[0]}{last}" if first else "",
        first,
        last,
    ] if v]
    return bool(variants) and max(fuzz.ratio(local.lower(), v) for v in variants) >= 75


def compute_confidence_score(
    email: str,
    candidate_domain: str | None,
    strategy: str,
    verdict: str,
    agent_name: str = "",
) -> int:
    """Compute additive confidence score for a validated email.

    With strategy  (max 4): domain match, name match, not generic, not catch-all
    Without strategy (max 3): domain match, IS generic, not catch-all
    """
    local, _, domain = email.partition("@")
    score = 0

    # +1 domain fuzzy-matches candidate domain (≥85)
    if candidate_domain:
        d_norm = domain.rsplit(".", 1)[0].replace("-", "") if "." in domain else domain
        c_norm = candidate_domain.rsplit(".", 1)[0].replace("-", "") if "." in candidate_domain else candidate_domain
        if fuzz.ratio(d_norm, c_norm) >= 85:
            score += 1

    if strategy == "with":
        # +1 local part fuzzy-matches agent name
        if agent_name and _name_matches_email(local, agent_name):
            score += 1
        # +1 local part is NOT a generic prefix
        if local.lower() not in _GENERIC_PREFIXES:
            score += 1
        # +1 not catch-all
        if verdict == "valid":
            score += 1
    else:
        # +1 local part IS a known generic prefix
        if local.lower() in _GENERIC_PREFIXES:
            score += 1
        # +1 not catch-all
        if verdict == "valid":
            score += 1

    return score


def confidence_tier(score: int) -> str:
    if score >= 3:
        return "high"
    if score == 2:
        return "medium"
    return "low"

