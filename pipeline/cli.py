from __future__ import annotations

import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="USX Email Discovery & Validation Pipeline v2",
    )
    subparsers = parser.add_subparsers(dest="subcommand")

    # --- run (default) ---
    run_parser = subparsers.add_parser("run", help="Execute the pipeline")
    _add_run_flags(run_parser)

    # Also accept run flags directly on the root parser (no subcommand = run)
    _add_run_flags(parser)

    # --- status ---
    status_parser = subparsers.add_parser("status", help="Show pipeline status")
    status_parser.add_argument("--db", default="bucket/pipeline.db", help="Database path")
    status_parser.add_argument("--watch", type=int, default=None, metavar="N",
                               help="Refresh every N seconds")

    # --- reset ---
    reset_parser = subparsers.add_parser("reset", help="Re-queue failed records")
    reset_parser.add_argument("--db", default="bucket/pipeline.db", help="Database path")
    reset_parser.add_argument("--status", default="discovery_failed",
                              choices=["discovery_failed", "validation_failed"],
                              help="Which status to re-queue")
    reset_parser.add_argument("--phase", default=None,
                              choices=["dns", "serper", "brave", "zuhal"],
                              help="Filter by failure phase")
    reset_parser.add_argument("--dry-run", action="store_true",
                              help="Print count without making changes")

    return parser


def _add_run_flags(parser: argparse.ArgumentParser) -> None:
    # I/O
    parser.add_argument("-i", "--input", dest="input_path", help="Input JSONL file path")
    parser.add_argument("--name", default=None,
                        help="Run name — outputs go to bucket/<name>/. Omit to use bucket/ directly (overwrites).")
    parser.add_argument("-o", "--output-dir", default=None, help="Override output directory")
    parser.add_argument("--db", default=None, help="Override SQLite database path")
    parser.add_argument("--log-dir", default=None, help="Override log directory")

    # Scope
    parser.add_argument("--limit", type=int, default=None, help="Max records to process")
    parser.add_argument("--start-offset", type=int, default=0, help="Skip first N lines")
    parser.add_argument("--ignore-checkpoint", action="store_true",
                        help="Ignore saved checkpoint, start from --start-offset")
    parser.add_argument("--chunk-size", type=int, default=100, help="Records per chunk (10-10000)")

    # Worker mode
    parser.add_argument("--producer-only", action="store_true", help="Run producer only")
    parser.add_argument("--consumer-only", action="store_true", help="Run consumer only")

    # Strategy
    parser.add_argument("--strategy", default="auto", choices=["auto", "with", "without"],
                        help="Email discovery strategy")

    # Concurrency
    parser.add_argument("--dns-concurrency", type=int, default=20, help="DNS semaphore size")
    parser.add_argument("--serper-concurrency", type=int, default=10, help="Serper semaphore size")
    parser.add_argument("--zuhal-concurrency", type=int, default=3, help="Zuhal semaphore size")

    # Rate limits
    parser.add_argument("--zuhal-rate-limit", type=int, default=200,
                        help="Zuhal calls/hour ceiling")
    parser.add_argument("--serper-rate-limit", type=int, default=500,
                        help="Serper calls/hour ceiling")
    parser.add_argument("--brave-rate-limit", type=int, default=500,
                        help="Brave calls/hour ceiling")
    parser.add_argument("--consumer-poll-interval", type=int, default=5,
                        help="Consumer poll interval (seconds)")

    # Backoff
    parser.add_argument("--max-attempts", type=int, default=3, help="Max retries per phase")
    parser.add_argument("--backoff-base-dns", type=float, default=0.5)
    parser.add_argument("--backoff-base-serper", type=float, default=1.0)
    parser.add_argument("--backoff-base-zuhal", type=float, default=1.0)
    parser.add_argument("--backoff-max-dns", type=float, default=8.0)
    parser.add_argument("--backoff-max-serper", type=float, default=32.0)
    parser.add_argument("--backoff-max-zuhal", type=float, default=64.0)
    parser.add_argument("--backoff-jitter", type=float, default=0.2)

    # Cost / safety
    parser.add_argument("--max-cost", type=float, default=None, help="USD cost ceiling")
    parser.add_argument("--dry-run", action="store_true", help="Mock all API calls")
    parser.add_argument("--yes", action="store_true",
                        help="Acknowledge warnings (e.g. high zuhal concurrency)")

    # Enrichment
    parser.add_argument("--enrichment-source", default="serper",
                        choices=["serper", "brave", "both"],
                        help="Phase 2 search API source")

    # Run ID
    parser.add_argument("--run-id", default="", help="Run identifier for stats")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Default subcommand to "run"
    if args.subcommand is None:
        args.subcommand = "run"

    # Validate: run subcommand requires input unless consumer-only
    if args.subcommand == "run":
        if not getattr(args, "consumer_only", False) and not getattr(args, "input_path", None):
            parser.error("-i/--input is required for the run subcommand (unless --consumer-only)")

        # Zuhal concurrency warning
        zuhal_conc = getattr(args, "zuhal_concurrency", 3)
        if zuhal_conc > 10 and not getattr(args, "yes", False):
            parser.error(
                f"--zuhal-concurrency {zuhal_conc} exceeds safe ceiling of 10. "
                "Pass --yes to acknowledge."
            )

    return args
