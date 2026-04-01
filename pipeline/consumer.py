from __future__ import annotations

import asyncio
import json
import logging

import aiosqlite

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
                    continue

                last_verdict = result.verdict

                if result.verdict in ("valid", "accept-all"):
                    await db.update_record_status(
                        self.conn,
                        unique_id,
                        "validated",
                        candidate_email=email,
                        zuhal_status=result.verdict,
                        zuhal_score=result.score,
                    )
                    logger.info("Validated: %s -> %s (%s)", unique_id, email, result.verdict)
                    return

                if result.verdict in ("invalid", "disposable"):
                    logger.debug(
                        "Candidate %s for %s: %s — trying next",
                        email, unique_id, result.verdict,
                    )
                    continue

                if result.verdict == "unknown":
                    if row["strategy"] == "with":
                        validated = await self._retry_unknown(unique_id, email, idx)
                        if validated:
                            return
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

    async def _retry_unknown(self, unique_id: str, email: str, attempt_base: int) -> bool:
        """Retry an 'unknown' verdict up to max_attempts. Returns True if validated."""
        for retry in range(1, self.config.max_attempts):
            try:
                result = await self.zuhal.validate(email)
                self.cost_tracker.record_call("zuhal")
            except PipelineHaltError:
                raise
            except Exception as exc:
                await db.insert_failure(
                    self.conn, unique_id, "zuhal",
                    attempt_base + retry + 1,
                    type(exc).__name__, str(exc),
                )
                continue

            if result.verdict in ("valid", "accept-all"):
                await db.update_record_status(
                    self.conn,
                    unique_id,
                    "validated",
                    candidate_email=email,
                    zuhal_status=result.verdict,
                    zuhal_score=result.score,
                )
                logger.info("Validated on retry: %s -> %s (%s)", unique_id, email, result.verdict)
                return True

            if result.verdict in ("invalid", "disposable"):
                return False

            logger.debug(
                "Still unknown for %s (%s), retry %d/%d",
                unique_id, email, retry, self.config.max_attempts - 1,
            )

        return False
