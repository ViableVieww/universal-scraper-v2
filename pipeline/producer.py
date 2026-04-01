from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from pathlib import Path

import aiohttp
import aiosqlite

from pipeline.config import PipelineConfig
from pipeline.models import InputRecord, PipelineHaltError
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.dns_probe import probe_domains
from pipeline.utils.email_patterns import generate_ranked_candidates
from pipeline.utils.rate_limiter import TokenBucket
from pipeline.utils.serper_client import SerperClient
from pipeline.utils.brave_client import BraveClient
from pipeline.utils.text import assign_email_strategy, is_org_agent, parse_name
from pipeline import db

logger = logging.getLogger("pipeline.producer")


def _is_transient_enrichment_error(exc: Exception) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, aiohttp.ClientConnectionError)):
        return True
    msg = str(exc)
    return any(code in msg for code in ("HTTP 429", "HTTP 500", "HTTP 503"))


class ProducerWorker:
    def __init__(
        self,
        config: PipelineConfig,
        conn: aiosqlite.Connection,
        cost_tracker: CostTracker,
        session: aiohttp.ClientSession,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self.config = config
        self.conn = conn
        self.cost_tracker = cost_tracker
        self.session = session
        self.stop_event = stop_event or asyncio.Event()

        self._dns_sem = asyncio.Semaphore(config.dns_concurrency)
        self._enrichment_sem = asyncio.Semaphore(config.serper_concurrency)

        _serper_bucket = TokenBucket(
            capacity=config.serper_rate_limit,
            refill_rate=config.serper_rate_limit / 3600,
        )
        _brave_bucket = TokenBucket(
            capacity=config.brave_rate_limit,
            refill_rate=config.brave_rate_limit / 3600,
        )

        self._serper = SerperClient(
            config.serper_api_key, session, _serper_bucket,
            dry_run=config.dry_run,
            max_attempts=config.max_attempts,
            jitter=config.backoff_jitter,
        )
        self._brave = BraveClient(
            config.brave_api_key, session, _brave_bucket,
            dry_run=config.dry_run,
            max_attempts=config.max_attempts,
            jitter=config.backoff_jitter,
        )

    async def run(self) -> None:
        config = self.config

        # Determine start offset
        if config.ignore_checkpoint:
            offset = config.start_offset
        else:
            saved = await db.get_checkpoint(self.conn, "producer_offset")
            offset = int(saved) if saved else config.start_offset

        logger.info("Producer starting at offset %d", offset)

        input_path = Path(config.input_path)
        if not input_path.exists():
            logger.error("Input file not found: %s", input_path)
            return

        total_processed = 0

        with open(input_path, "r", encoding="utf-8") as f:
            # Skip to offset
            for _ in range(offset):
                line = f.readline()
                if not line:
                    logger.info("Input file shorter than offset %d — nothing to do", offset)
                    await db.upsert_checkpoint(self.conn, "producer_done", "true")
                    return

            while not self.stop_event.is_set():
                chunk: list[InputRecord] = []

                # Respect --limit: only read as many as needed
                remaining = config.limit - total_processed if config.limit else config.chunk_size
                read_size = min(config.chunk_size, remaining)

                for _ in range(read_size):
                    line = f.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = InputRecord.from_dict(json.loads(line))
                        chunk.append(record)
                    except (json.JSONDecodeError, KeyError) as exc:
                        logger.warning("Skipping malformed line at offset %d: %s", offset, exc)

                if not chunk:
                    logger.info("Producer exhausted input file at offset %d", offset)
                    break

                # Process chunk concurrently
                results = await self._process_chunk(chunk)

                # Atomic write + checkpoint advance
                new_offset = offset + len(chunk)
                await db.insert_records_batch(self.conn, results, new_offset)
                offset = new_offset

                total_processed += len(chunk)
                logger.info(
                    "Chunk written: %d records, offset now %d, total processed %d",
                    len(chunk), offset, total_processed,
                )

                # Check cost ceiling
                if self.cost_tracker.ceiling_reached():
                    logger.warning("Cost ceiling reached — producer stopping")
                    break

                # Check limit
                if config.limit and total_processed >= config.limit:
                    logger.info("Producer limit reached (%d/%d records)", total_processed, config.limit)
                    break

        if not self.stop_event.is_set() and self.config.max_discovery_retries > 0:
            await self._run_discovery_retries()

        await db.upsert_checkpoint(self.conn, "producer_done", "true")
        logger.info("Producer finished. Total processed: %d", total_processed)

    async def _run_discovery_retries(self) -> None:
        for attempt in range(1, self.config.max_discovery_retries + 1):
            rows = await db.fetch_pending_discovery(self.conn, limit=self.config.chunk_size)
            if not rows:
                logger.info("Discovery retry %d: nothing pending — done", attempt)
                break

            logger.info(
                "Discovery retry %d/%d: %d records",
                attempt, self.config.max_discovery_retries, len(rows),
            )

            attempts_by_id = {row["unique_id"]: (row["discovery_attempts"] or 1) for row in rows}

            records = [
                InputRecord(
                    unique_id=row["unique_id"],
                    business_name=row["business_name"] or "",
                    agent_name=row["agent_name"] or "",
                    state=row["state"] or "",
                    jurisdiction=row["jurisdiction"] or "",
                    position_type=row["position_type"] or "",
                    name_entity_type=row["name_entity_type"] or "",
                )
                for row in rows
            ]

            results = await asyncio.gather(*[self._process_record(r) for r in records])

            for result in results:
                result["discovery_attempts"] = attempts_by_id.get(result["unique_id"], 1) + 1
                await db.update_record_discovery(self.conn, result)

            if self.cost_tracker.ceiling_reached():
                break

    async def _process_chunk(self, chunk: list[InputRecord]) -> list[dict]:
        tasks = [self._process_record(record) for record in chunk]
        results = await asyncio.gather(*tasks)
        counts = Counter(r["status"] for r in results)
        logger.info(
            "Chunk buckets — pv:%d pd:%d df:%d",
            counts.get("pending_validation", 0),
            counts.get("pending_discovery", 0),
            counts.get("discovery_failed", 0),
        )
        return results

    async def _process_record(self, record: InputRecord) -> dict:
        config = self.config

        # Determine strategy
        if config.strategy == "auto":
            strategy = assign_email_strategy(record)
        else:
            strategy = config.strategy

        org_agent = is_org_agent(record)
        result = self._base_result(record, strategy, org_agent)

        # Short-circuit: existing email
        existing_email = record.email_biz or record.email_agent
        if existing_email:
            result["candidate_email"] = existing_email
            result["candidate_emails"] = json.dumps([existing_email])
            result["discovery_source"] = "input"
            result["status"] = "pending_validation"
            return result

        # Phase 1: DNS probe
        domain, mx_host = await probe_domains(
            record.business_name,
            self._dns_sem,
            max_attempts=config.max_attempts,
            jitter=config.backoff_jitter,
            dry_run=config.dry_run,
        )

        candidate_emails: list[str] = []
        had_transient_error = False
        transient_error_source = ""

        # Parse name once for both pattern generation and search queries
        first, _, last = parse_name(record.agent_name)
        parsed_agent = f"{first} {last}".strip() if first and last else record.agent_name

        if domain:
            result["candidate_domain"] = domain
            result["discovery_source"] = "dns"

            # Generate ranked patterns from domain
            patterns = generate_ranked_candidates(first, last, domain, strategy)
            candidate_emails.extend(patterns)

        # Phase 2: Serper/Brave enrichment (if no domain or to find emails in snippets)
        enrichment_emails: list[str] = []
        enrichment_domain: str | None = None

        if config.enrichment_source in ("serper", "both"):
            try:
                async with self._enrichment_sem:
                    serper_result = await self._serper.enrich(
                        record.business_name,
                        parsed_agent if strategy == "with" else None,
                        record.state,
                        domain,
                        strategy,
                    )
                    self.cost_tracker.record_call("serper")

                enrichment_emails.extend(serper_result.candidate_emails)
                if not domain and serper_result.candidate_domain:
                    enrichment_domain = serper_result.candidate_domain
                    result["discovery_source"] = "serper"

            except PipelineHaltError:
                raise
            except Exception as exc:
                if _is_transient_enrichment_error(exc):
                    had_transient_error = True
                    transient_error_source = "serper"
                    logger.warning("Serper transient error for %s: %s", record.unique_id, exc)
                else:
                    logger.warning("Serper error for %s: %s", record.unique_id, exc)

        # Brave fallback
        if (
            not enrichment_emails
            and not enrichment_domain
            and config.enrichment_source in ("brave", "both")
        ):
            try:
                async with self._enrichment_sem:
                    brave_result = await self._brave.enrich(
                        record.business_name,
                        parsed_agent if strategy == "with" else None,
                        record.state,
                        domain or enrichment_domain,
                        strategy,
                    )
                    self.cost_tracker.record_call("brave")

                enrichment_emails.extend(brave_result.candidate_emails)
                if not domain and not enrichment_domain and brave_result.candidate_domain:
                    enrichment_domain = brave_result.candidate_domain
                    result["discovery_source"] = "brave"

            except PipelineHaltError:
                raise
            except Exception as exc:
                if _is_transient_enrichment_error(exc):
                    had_transient_error = True
                    transient_error_source = "brave"
                    logger.warning("Brave transient error for %s: %s", record.unique_id, exc)
                else:
                    logger.warning("Brave error for %s: %s", record.unique_id, exc)

        # If enrichment found a domain but DNS didn't, generate patterns from it
        if not domain and enrichment_domain:
            result["candidate_domain"] = enrichment_domain
            patterns = generate_ranked_candidates(first, last, enrichment_domain, strategy)
            candidate_emails.extend(patterns)

        # Prepend any emails found directly in search snippets (they're already validated-looking)
        all_candidates: list[str] = []
        seen: set[str] = set()
        for email in enrichment_emails + candidate_emails:
            lower = email.lower()
            if lower not in seen:
                seen.add(lower)
                all_candidates.append(lower)

        # Cap at reasonable limit
        all_candidates = all_candidates[:10]

        effective_domain = domain or enrichment_domain
        result["discovery_attempts"] = 1

        if all_candidates:
            result["candidate_emails"] = json.dumps(all_candidates)
            result["candidate_email"] = all_candidates[0]
            result["status"] = "pending_validation"
            logger.info(
                "BUCKET pv | %s | domain=%s emails=%d",
                record.unique_id, effective_domain, len(all_candidates),
            )
        elif had_transient_error and not effective_domain:
            result["status"] = "pending_discovery"
            logger.warning(
                "BUCKET pd | %s | transient=%s",
                record.unique_id, transient_error_source,
            )
        else:
            result["status"] = "discovery_failed"
            logger.info(
                "BUCKET df | %s | domain=%s",
                record.unique_id, effective_domain,
            )

        return result

    @staticmethod
    def _base_result(record: InputRecord, strategy: str, org_agent: bool) -> dict:
        return {
            "unique_id": record.unique_id,
            "business_name": record.business_name,
            "agent_name": record.agent_name,
            "state": record.state,
            "jurisdiction": record.jurisdiction,
            "position_type": record.position_type,
            "name_entity_type": record.name_entity_type,
            "candidate_email": None,
            "candidate_emails": None,
            "candidate_domain": None,
            "discovery_source": None,
            "discovery_attempts": 0,
            "strategy": strategy,
            "is_org_agent": org_agent,
            "status": "queued",
        }
