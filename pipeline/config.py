from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- API Keys (from .env only) ---
    serper_api_key: str = ""
    brave_api_key: str = ""
    zuhal_api_key: str = ""

    # --- I/O ---
    input_path: Path = Path("records/input.jsonl")
    output_dir: Path = Path("bucket")
    db_path: Path = Path("bucket/pipeline.db")
    log_dir: Path = Path("bucket/logs")

    # --- Scope ---
    limit: int | None = None
    start_offset: int = 0
    ignore_checkpoint: bool = False
    chunk_size: int = Field(default=100, ge=10, le=10000)

    # --- Worker mode ---
    producer_only: bool = False
    consumer_only: bool = False

    # --- Strategy ---
    strategy: Literal["auto", "with", "without"] = "auto"

    # --- Concurrency ---
    dns_concurrency: int = Field(default=20, le=50)
    serper_concurrency: int = 10
    zuhal_concurrency: int = Field(default=3)

    # --- Rate limits (calls per hour) ---
    zuhal_rate_limit: int = 200
    serper_rate_limit: int = 500
    brave_rate_limit: int = 500
    consumer_poll_interval: int = 5

    # --- Backoff ---
    max_attempts: int = 3
    backoff_base_dns: float = 0.5
    backoff_base_serper: float = 1.0
    backoff_base_zuhal: float = 1.0
    backoff_max_dns: float = 8.0
    backoff_max_serper: float = 32.0
    backoff_max_zuhal: float = 64.0
    backoff_jitter: float = 0.2

    # --- Cost / safety ---
    max_cost: float | None = None
    dry_run: bool = False
    max_consecutive_errors: int = Field(default=10, ge=1)
    max_discovery_retries: int = Field(default=3, ge=0)

    # --- Enrichment ---
    enrichment_source: Literal["serper", "brave", "both"] = "serper"

    # --- Run identity ---
    run_id: str = ""

    @model_validator(mode="after")
    def _validate_flags(self) -> PipelineConfig:
        if self.producer_only and self.consumer_only:
            raise ValueError("--producer-only and --consumer-only are mutually exclusive")

        if self.zuhal_concurrency > 10:
            warnings.warn(
                f"zuhal_concurrency={self.zuhal_concurrency} exceeds safe ceiling of 10. "
                "Pass --yes to acknowledge.",
                stacklevel=2,
            )

        if self.ignore_checkpoint and self.start_offset == 0:
            logging.getLogger("pipeline").warning(
                "--ignore-checkpoint without --start-offset is a no-op (starts from line 0)"
            )

        if self.consumer_only and self.strategy != "auto":
            logging.getLogger("pipeline").warning(
                f"--strategy {self.strategy} has no effect with --consumer-only"
            )

        return self
