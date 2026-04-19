# USX — Email Discovery & Validation Pipeline

A production-grade, async Python pipeline that discovers and validates professional email addresses from US business entity records. Given a business name, registered agent, and state, USX works through a three-phase enrichment sequence to surface a validated contact email — at scale, on a VPS, without burning your IP.

---

## How It Works

USX is built around a deliberate architectural decision: **discovery and validation are decoupled**. Most email pipelines sequence these operations per-record, which means a slow validator blocks a fast discoverer. USX runs them as two independent async workers sharing a SQLite database.

The **Producer Worker** ingests records in chunks of 100, runs DNS probing against the business domain, falls back to Serper or Brave Search if DNS yields nothing, and writes candidate emails to SQLite. It never waits for validation — it just keeps moving.

The **Consumer Worker** runs in the background, polling SQLite for candidates marked `pending_validation`, and passes each one through Zuhal for email verification. It is intentionally rate-capped at 200 calls/hour via a token bucket, with random inter-call throttling to avoid triggering anti-abuse systems. On a large run, the consumer may still be draining its queue hours after the producer has finished.

This split exists because Phase 3 (validation) touches external mail servers. That is the operation that caused the previous VPS to be blacklisted. Isolating it behind a token bucket and giving it its own failure budget is a risk-isolation strategy, not just a performance one.

```text
INPUT JSONL
     │
     ▼
┌─────────────────────────────┐
│       PRODUCER WORKER       │
│                             │
│  Phase 1 — DNS Probe        │  ← Semaphore: 20
│  Phase 2 — Serper / Brave   │  ← Semaphore: 10 (fallback only)
│                             │
│  Writes: pending_validation │
│  Checkpoint: every 100 rows │
└─────────────────────────────┘
              │
         SQLite DB
              │
┌─────────────────────────────┐
│       CONSUMER WORKER       │
│                             │
│  Phase 3 — Zuhal Validate   │  ← Semaphore: 3, 200 calls/hr cap
│                             │
│  Writes: validated |        │
│          validation_failed  │
└─────────────────────────────┘
     │
     ▼
OUTPUT (valid_emails.csv, results.jsonl, report.json)
```

---

## Input Format

Records are supplied as newline-delimited JSON (`.jsonl`). Each line must conform to this shape:

```jsonl
{
  "unique_id": "CA-202462813867",
  "business_name": "PHP for pharmaceutical industries LLC",
  "agent_name": "PHP for pharmaceutical industries LLC",
  "state": "CA",
  "jurisdiction": null,
  "position": "Registered Agent",
  "position_type": "Agent",
  "name_entity_type": "Organization",
  "email_biz": "",
  "email_agent": null,
  "unique_agent_id": null
}
```

Records where `email_biz` is non-empty are short-circuited directly to `pending_validation` — discovery is skipped and Zuhal validates the existing address. Records where `name_entity_type` is `"Organization"` (agent name equals business name) skip officer-specific email pattern generation and use a generic contact search instead.

---

## Installation

Python 3.10 or higher is required.

```bash
git clone https://github.com/vvJerome/us-z-2.git
cd us-z-2
pip install -r requirements.txt
cp .env.example .env
# Fill in your API keys in .env
```

---

## Configuration

All runtime configuration lives in `.env`. The pipeline validates every value at startup via Pydantic v2 — a missing key or out-of-range value raises an explicit error before any network calls are made.

```env
# API Keys
SERPER_API_KEY=your_key_here
BRAVE_API_KEY=your_key_here
ZUHAL_API_KEY=your_key_here

# Concurrency — adjust per VPS resource profile
DNS_SEMAPHORE=20
SERPER_SEMAPHORE=10
ZUHAL_SEMAPHORE=3

# Rate limiting
ZUHAL_MAX_CALLS_PER_HOUR=200

# Pipeline behaviour
CHUNK_SIZE=100
CONSUMER_POLL_INTERVAL_SECONDS=5
MAX_ATTEMPTS=3

# Paths
INPUT_JSONL=records/input.jsonl
OUTPUT_DIR=output/
DB_PATH=pipeline.db
```

Start conservative on `ZUHAL_MAX_CALLS_PER_HOUR`. If Zuhal begins returning HTTP 429 responses, the consumer triggers a 10-minute circuit-breaker cooldown and logs the event clearly — that is your signal to lower the ceiling.

---

## Running the Pipeline

```bash
# Standard run — both workers launch together
python -m pipeline -i records/input.jsonl

# Producer only — populate the DB without touching Zuhal
python -m pipeline -i records/input.jsonl --producer-only

# Consumer only — drain an existing pending_validation queue
python -m pipeline --consumer-only

# Test run — process first 50 records only
python -m pipeline -i records/input.jsonl --limit 50

# Resume from a specific record offset (e.g. after a crash)
python -m pipeline -i records/input.jsonl --start-offset 4200

# Dry run — full pipeline logic, no external API calls
python -m pipeline -i records/input.jsonl --dry-run

# Custom output directory
python -m pipeline -i records/input.jsonl -o output/run_20240401/
```

The pipeline exits when both workers stop — which in a normal run means the producer has exhausted the input file and the consumer has drained the `pending_validation` queue to zero.

---

## Output

```text
output/
  valid_emails.csv      — validated emails only, with metadata
  results.jsonl         — all records with full pipeline metadata
  report.json           — hit rate, cost estimate, tier breakdown
```

The SQLite database (`pipeline.db`) is the canonical source of truth throughout the run. The CSV and JSONL outputs are derived exports written at the end of a run or on demand.

---

## Monitoring a Live Run

Query the database directly at any time to check pipeline state:

```bash
# Overall status breakdown
sqlite3 pipeline.db "SELECT status, COUNT(*) FROM records GROUP BY status;"

# Current producer position
sqlite3 pipeline.db "SELECT value FROM checkpoints WHERE key = 'producer_offset';"

# Failure audit for a specific record
sqlite3 pipeline.db "SELECT * FROM failures WHERE unique_id = 'CA-202462813867';"

# Estimated Zuhal call volume this run
sqlite3 pipeline.db "SELECT zuhal_calls FROM stats;"
```

---

## Failure Handling

Every phase is given `MAX_ATTEMPTS = 3` retries with exponential backoff and ±20% jitter before a record is marked failed. Failure records are never deleted — they are parked with a `discovery_failed` or `validation_failed` status and logged to the `failures` table for audit.

To re-queue all discovery failures for a retry pass:

```sql
UPDATE records
SET status = 'queued', discovery_attempts = 0
WHERE status = 'discovery_failed';
```

Then re-run with `--producer-only` to pick them up without touching already-validated records.

---

## Database Schema (Quick Reference)

The `records` table drives the state machine. The `status` column is the handoff point between the two workers:

```text
queued → pending_validation → validated
       → discovery_failed
                           → validation_failed
```

The `checkpoints` table stores `producer_offset` (for cold-restart safety) and `producer_done` (flipped when the input JSONL is exhausted). The `failures` table is append-only — every failed attempt is logged with its phase, attempt number, and error detail.

---

## SMTP Verification via SIM (verify_emails.py)

USX ships a second validation approach alongside Zuhal: **direct SMTP verification routed through a SIM-bound device**. This is implemented in `verify_emails.py` and `check_status.py`, and is designed to run against an already-populated `pipeline.db` where records are in `pending_validation` state.

### Why a second approach?

Zuhal is a managed API — it abstracts away the SMTP handshake but introduces rate caps, cost per call, and a third-party dependency. The SIM-based approach does the SMTP verification directly (`RCPT TO` check) from a device or VPS whose outbound IP has port 25 open. It is cheaper at scale and avoids Zuhal's 200 calls/hr ceiling, but requires a carrier or ISP that does not block port 25.

### How it works

```text
pipeline.db (pending_validation records)
     │
     ▼
┌─────────────────────────────────────┐
│           verify_emails.py          │
│                                     │
│  1. Read 500 records at a time      │
│  2. POST /jobs/batch to verifier    │  ← email-verifier.bbops.io
│  3. Poll GET /batches/{id} until    │
│     done (10s → 120s backoff,       │
│     30 min timeout + requeue)       │
│  4. Pick best result per record:    │
│     valid > catch_all > error >     │
│     invalid                         │
│  5. Update pipeline.db + write CSV  │
└─────────────────────────────────────┘
     │
     ▼
valid_emails.csv + pipeline.db updated
```

The verifier server (`email-verifier.bbops.io`) manages a job queue. Workers — Android devices or PCs with port 25 open on a SIM or ISP — pull jobs, run SMTP `RCPT TO` checks, and post results back. `verify_emails.py` is the client that feeds jobs in and collects results out.

### Running the SMTP verifier

```bash
# Single run — processes all pending_validation records and exits
PIPELINE_DB=/path/to/pipeline.db OUTPUT_CSV=/path/to/valid_emails.csv python3 verify_emails.py

# With explicit flags
python3 verify_emails.py --db pipeline.db --out valid_emails.csv
```

The script is resume-safe — a `verifier_jobs` table is added to `pipeline.db` on first run, and any subsequent run skips emails already submitted. If interrupted mid-run, restart with the same command and it picks up where it left off.

### Monitoring a live run (VPS)

`check_status.py` connects to the VPS over SSH and prints a live status snapshot:

```bash
# Single check
python check_status.py

# Live watch — refreshes every 60s
python check_status.py --watch

# Custom interval
python check_status.py --watch --interval 30
```

Output includes processed/remaining counts, SMTP valid vs accept-all breakdown, and last log lines from the VPS.

### Validation result states

| Status | Meaning |
| --- | --- |
| `valid` | SMTP server accepted the recipient — mailbox confirmed |
| `catch_all` (accept-all) | Domain accepts all addresses — mailbox-level validity uncertain |
| `invalid` | SMTP server rejected the recipient (550/551/552/553/554) |
| `error` | Network error, port 25 blocked, or Spamhaus rejection |

Records where the best result is `valid` or `catch_all` are marked `validated` in `pipeline.db`. All others are marked `validation_failed`.

### Comparison: Zuhal vs SMTP/SIM

| | Zuhal | SMTP / SIM |
| --- | --- | --- |
| Cost | Per-call API fee | Infrastructure only |
| Rate | 200 calls/hr cap | Worker-bound (no API limit) |
| Setup | API key only | Requires port 25 open on worker |
| Accuracy | High (managed service) | High (direct SMTP handshake) |
| Blacklist risk | Managed by Zuhal | Depends on worker IP reputation |
| Resume support | Via `pipeline.db` status | Via `verifier_jobs` table |

---

## Architecture Notes

The full architecture decision record lives in `PIPELINE_ARCHITECTURE.md`. Key decisions worth knowing before modifying this codebase:

**Why no Hunter.io or NeverBounce?** Three services (DNS, Serper/Brave, Zuhal) is the intentional scope. Adding more validators introduces cost overlap and complicates the failure budget without meaningfully improving precision at this stage.

**Why SQLite and not Postgres?** This runs on a single VPS. SQLite in WAL mode handles the producer/consumer concurrency pattern cleanly without requiring a separate database process. It is also trivially backed up with a single file copy.

**Why is the Zuhal semaphore only 3?** Zuhal validation triggers SMTP handshakes against external mail servers. High concurrency against the same domain's MX server in a short window is exactly the pattern that causes IP blacklisting. 3 concurrent calls with a 200/hr ceiling keeps the traffic profile indistinguishable from human browsing.

---

## Tech Stack

| Dependency | Purpose |
| --- | --- |
| `aiohttp` | Async HTTP for Serper, Brave, and Zuhal API calls |
| `aiosqlite` | Async SQLite writes from both workers |
| `aiodns` | Async DNS resolution for Phase 1 probing |
| `pydantic >= 2.0` | Config validation and record models |
| `pydantic-settings` | `.env` loading with type coercion |
| `fuzzywuzzy` + `python-Levenshtein` | Domain fuzzy-matching for candidate scoring |
| `python-dotenv` | Environment file loading |

---

## Repository Structure

```text
us-z-2/
├── pipeline/
│   ├── __main__.py          # Entry point — launches both workers via asyncio.gather
│   ├── producer.py          # ProducerWorker: DNS + Serper/Brave + chunk writes
│   ├── consumer.py          # ConsumerWorker: Zuhal polling loop
│   ├── db.py                # SQLite init, schema, WAL configuration
│   ├── config.py            # PipelineConfig (Pydantic v2 settings)
│   ├── models.py            # InputRecord, CandidateRecord, ValidationResult
│   └── utils/
│       ├── dns_probe.py     # Async DNS stem probe + MX lookup
│       ├── serper_client.py # Serper API wrapper with backoff
│       ├── brave_client.py  # Brave Search API wrapper with backoff
│       ├── zuhal_client.py  # Zuhal API wrapper + token bucket integration
│       ├── rate_limiter.py  # TokenBucket (200 calls/hr ceiling)
│       ├── backoff.py       # Exponential backoff + jitter
│       ├── email_patterns.py # Pattern permutation generator
│       ├── text.py          # Name parsing and normalisation
│       └── logger.py        # Structured JSON logging
├── records/                 # Input JSONL files (gitignored)
├── output/                  # Pipeline output (gitignored)
├── logs/                    # Structured log files (gitignored)
├── PIPELINE_ARCHITECTURE.md # Full architecture context for Claude Code
├── .env.example
├── .gitignore
└── requirements.txt
```
