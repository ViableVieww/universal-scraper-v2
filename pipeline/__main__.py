from __future__ import annotations

import asyncio
import csv
import json
import logging
import signal
import sys
import time
from pathlib import Path

import aiohttp

from pipeline.cli import parse_args
from pipeline.config import PipelineConfig
from pipeline.consumer import ConsumerWorker
from pipeline.producer import ProducerWorker
from pipeline.utils.cost_tracker import CostTracker
from pipeline.utils.logger import setup_logging, get_logger
from pipeline.utils.rate_limiter import CircuitBreaker, TokenBucket
from pipeline.utils.zuhal_client import ZuhalClient
from pipeline import db


async def cmd_run(args, config: PipelineConfig) -> None:
    """Execute the pipeline (producer + consumer or one of them)."""
    setup_logging(config)
    logger = get_logger("pipeline")

    conn = await db.init_db(config.db_path)
    logger.info("Database initialized: %s", config.db_path)

    stop_event = asyncio.Event()

    # Graceful shutdown via signals
    def _signal_handler():
        logger.info("Shutdown signal received — stopping workers gracefully")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for all signals
            pass

    session = aiohttp.ClientSession()
    cost_tracker = CostTracker(config.max_cost)
    base_run_id = config.run_id or f"run_{int(time.time())}"
    # In split mode (producer-only or consumer-only), tag the run_id with the
    # worker role so both processes write to separate stats rows and don't
    # overwrite each other on the shared database.
    if config.producer_only:
        run_id = f"{base_run_id}-producer"
    elif config.consumer_only:
        run_id = f"{base_run_id}-consumer"
    else:
        run_id = base_run_id

    tasks: list[asyncio.Task] = []

    try:
        if not config.consumer_only:
            producer = ProducerWorker(config, conn, cost_tracker, session, stop_event)
            tasks.append(asyncio.create_task(producer.run(), name="producer"))
            logger.info("Producer worker started")

        if not config.producer_only:
            bucket = TokenBucket(
                capacity=config.zuhal_rate_limit,
                refill_rate=config.zuhal_rate_limit / 3600,
            )
            breaker = CircuitBreaker(cooldown_seconds=600)
            zuhal = ZuhalClient(
                config.zuhal_api_key, session, bucket, breaker,
                dry_run=config.dry_run,
                max_attempts=config.max_attempts,
                jitter=config.backoff_jitter,
            )
            consumer = ConsumerWorker(config, conn, cost_tracker, zuhal, stop_event)
            tasks.append(asyncio.create_task(consumer.run(), name="consumer"))
            logger.info("Consumer worker started")

        if not tasks:
            logger.error("No workers to run — check flags")
            return

        await asyncio.gather(*tasks)

    except Exception:
        logger.exception("Pipeline error")
        stop_event.set()
        raise
    finally:
        # Update stats
        await db.upsert_stats(
            conn, run_id,
            estimated_cost_usd=cost_tracker.total_cost,
            **{f"{k}_calls": v for k, v in cost_tracker.counts.items()},
        )

        # Write output files
        await _write_outputs(conn, config)

        await session.close()
        await conn.close()
        logger.info("Pipeline shutdown complete. Cost: $%.4f", cost_tracker.total_cost)


async def cmd_status(args) -> None:
    """Print pipeline status summary."""
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return

    conn = await db.init_db(db_path)

    while True:
        summary = await db.get_status_summary(conn)
        _print_status(summary)

        if not args.watch:
            break
        await asyncio.sleep(args.watch)

    await conn.close()


async def cmd_reset(args) -> None:
    """Re-queue failed records."""
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return

    conn = await db.init_db(db_path)

    if args.dry_run:
        # Count without modifying
        async with conn.execute(
            "SELECT COUNT(*) FROM records WHERE status = ?", (args.status,)
        ) as cursor:
            row = await cursor.fetchone()
            count = row[0] if row else 0
        print(f"Would re-queue {count} records with status '{args.status}'")
    else:
        count = await db.reset_failed_records(conn, args.status, args.phase)
        print(f"Re-queued {count} records")

    await conn.close()


def _print_status(summary: dict) -> None:
    print("\n=== Pipeline Status ===\n")
    print(f"Total records: {summary.get('total_records', 0)}")
    print(f"Producer offset: {summary.get('producer_offset', 0)}")
    print(f"Producer done: {summary.get('producer_done', False)}")

    print("\nRecords by status:")
    for status, count in sorted(summary.get("records_by_status", {}).items()):
        print(f"  {status:.<30} {count:>8}")

    failures = summary.get("failures_by_phase", {})
    if failures:
        print("\nFailures by phase:")
        for phase, count in sorted(failures.items()):
            print(f"  {phase:.<30} {count:>8}")

    stats = summary.get("stats")
    if stats:
        cost = stats.get("estimated_cost_usd", 0)
        print(f"\nEstimated cost: ${cost:.4f}")

    print()


async def _write_outputs(conn, config: PipelineConfig) -> None:
    """Write final output files (CSV, JSONL, report)."""
    logger = get_logger("pipeline")
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # valid_emails.csv
    csv_path = output_dir / "valid_emails.csv"
    async with conn.execute(
        "SELECT unique_id, business_name, agent_name, state, candidate_email, "
        "zuhal_status, zuhal_score FROM records WHERE status = 'validated'"
    ) as cursor:
        rows = await cursor.fetchall()

    if rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "unique_id", "business_name", "agent_name", "state",
                "email", "zuhal_status", "zuhal_score",
            ])
            for row in rows:
                writer.writerow([row[k] for k in [
                    "unique_id", "business_name", "agent_name", "state",
                    "candidate_email", "zuhal_status", "zuhal_score",
                ]])
        logger.info("Wrote %d validated emails to %s", len(rows), csv_path)

    # results.jsonl
    jsonl_path = output_dir / "results.jsonl"
    async with conn.execute("SELECT * FROM records") as cursor:
        all_rows = await cursor.fetchall()

    if all_rows:
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for row in all_rows:
                f.write(json.dumps(dict(row), default=str) + "\n")
        logger.info("Wrote %d records to %s", len(all_rows), jsonl_path)

    # report.json
    summary = await db.get_status_summary(conn)
    report_path = output_dir / "report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info("Wrote report to %s", report_path)


async def main() -> None:
    args = parse_args()

    if args.subcommand == "status":
        await cmd_status(args)
        return

    if args.subcommand == "reset":
        await cmd_reset(args)
        return

    # Build PipelineConfig from args
    config_kwargs: dict = {}
    for field_name in [
        "input_path", "output_dir", "db_path", "log_dir",
        "limit", "start_offset", "ignore_checkpoint", "chunk_size",
        "producer_only", "consumer_only", "strategy",
        "dns_concurrency", "serper_concurrency", "zuhal_concurrency",
        "zuhal_rate_limit", "serper_rate_limit", "consumer_poll_interval",
        "max_attempts", "backoff_base_dns", "backoff_base_serper", "backoff_base_zuhal",
        "backoff_max_dns", "backoff_max_serper", "backoff_max_zuhal", "backoff_jitter",
        "max_cost", "dry_run", "enrichment_source", "run_id",
    ]:
        val = getattr(args, field_name, None)
        if val is not None:
            config_kwargs[field_name] = val

    # Map --db to db_path, --output-dir to output_dir
    if hasattr(args, "db") and args.db:
        config_kwargs["db_path"] = args.db

    config = PipelineConfig(**config_kwargs)
    await cmd_run(args, config)


if __name__ == "__main__":
    asyncio.run(main())
