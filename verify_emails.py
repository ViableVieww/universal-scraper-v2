"""
verify_emails.py — Email Verification Pipeline
Reads pending_validation records from pipeline.db, submits all candidate emails
to email-verifier.bbops.io, polls for SMTP results, updates pipeline.db, and
produces valid_emails.csv.

Usage:
    python3 verify_emails.py [--db /path/to/pipeline.db] [--out /path/to/valid_emails.csv]
"""

import argparse
import csv
import json
import logging
import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL        = "https://email-verifier.bbops.io"
CHUNK_SIZE      = 500       # records loaded per iteration
BATCH_SIZE      = 1000      # max emails per POST /jobs/batch
POLL_START_S    = 10        # initial poll interval (seconds)
POLL_MAX_S      = 120       # max poll interval
POLL_TIMEOUT_S  = 1800      # 30 min — mark batch as timed-out after this
MAX_FAILURES    = 100       # consecutive API failures before aborting
BACKOFF_BASE_S  = 1         # initial retry backoff
BACKOFF_MAX_S   = 60        # max retry backoff
REQUEST_TIMEOUT = 30        # HTTP request timeout (seconds)

DB_PATH  = os.environ.get("PIPELINE_DB", "/root/pipeline.db")
CSV_PATH = os.environ.get("OUTPUT_CSV",  "/root/valid_emails.csv")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "verify.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger("verify")


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------
class TooManyFailuresError(Exception):
    pass


class EmailVerifierClient:
    def __init__(self, base_url: str, max_failures: int = MAX_FAILURES):
        self.base_url = base_url.rstrip("/")
        self.max_failures = max_failures
        self._consecutive_failures = 0
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    # -- internal helpers --------------------------------------------------

    def _check_failures(self):
        if self._consecutive_failures >= self.max_failures:
            raise TooManyFailuresError(
                f"Reached {self._consecutive_failures} consecutive API failures — aborting."
            )

    def _backoff(self, attempt: int) -> float:
        delay = min(BACKOFF_BASE_S * (2 ** attempt), BACKOFF_MAX_S)
        jitter = delay * 0.2 * random.random()
        return delay + jitter

    def _get(self, path: str, params: dict = None, max_retries: int = 5) -> dict:
        url = f"{self.base_url}{path}"
        for attempt in range(max_retries):
            self._check_failures()
            try:
                r = self._session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if r.status_code < 500:
                    self._consecutive_failures = 0
                    r.raise_for_status()
                    return r.json()
                # 5xx — retry
                log.warning("GET %s -> %d, retrying (%d/%d)", path, r.status_code, attempt + 1, max_retries)
                self._consecutive_failures += 1
            except (requests.ConnectionError, requests.Timeout) as e:
                log.warning("GET %s -> network error: %s, retrying (%d/%d)", path, e, attempt + 1, max_retries)
                self._consecutive_failures += 1
            except requests.HTTPError as e:
                self._consecutive_failures += 1
                raise
            time.sleep(self._backoff(attempt))
        self._consecutive_failures += 1
        raise RuntimeError(f"GET {path} failed after {max_retries} retries")

    def _post(self, path: str, payload: dict, max_retries: int = 5) -> dict:
        url = f"{self.base_url}{path}"
        for attempt in range(max_retries):
            self._check_failures()
            try:
                r = self._session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
                if r.status_code == 409:
                    # Conflict — transient, retry
                    log.warning("POST %s -> 409 conflict, retrying (%d/%d)", path, attempt + 1, max_retries)
                    self._consecutive_failures += 1
                    time.sleep(self._backoff(attempt))
                    continue
                if r.status_code < 500:
                    self._consecutive_failures = 0
                    r.raise_for_status()
                    return r.json()
                log.warning("POST %s -> %d, retrying (%d/%d)", path, r.status_code, attempt + 1, max_retries)
                self._consecutive_failures += 1
            except (requests.ConnectionError, requests.Timeout) as e:
                log.warning("POST %s -> network error: %s, retrying (%d/%d)", path, e, attempt + 1, max_retries)
                self._consecutive_failures += 1
            except requests.HTTPError as e:
                self._consecutive_failures += 1
                raise
            time.sleep(self._backoff(attempt))
        self._consecutive_failures += 1
        raise RuntimeError(f"POST {path} failed after {max_retries} retries")

    # -- public API --------------------------------------------------------

    def health_check(self):
        try:
            data = self._get("/health")
            if not data.get("ok"):
                raise RuntimeError(f"Health check returned not-ok: {data}")
            log.info("Health check OK: %s", data)
        except Exception as e:
            log.critical("Health check failed: %s", e)
            raise

    def submit_batch(self, emails: list) -> str:
        """Submit emails as a batch job. Returns batch_id."""
        data = self._post("/jobs/batch", {"emails": emails})
        batch_id = data["batch_id"]
        count = data["count"]
        auto_catch_all = data.get("auto_catch_all_count", 0)
        log.info("Submitted batch %s: %d jobs (auto catch-all: %d)", batch_id, count, auto_catch_all)
        return batch_id, data.get("jobs", [])

    def get_batch_status(self, batch_id: str) -> dict:
        return self._get(f"/batches/{batch_id}")

    def get_batch_jobs(self, batch_id: str, limit: int = 5000) -> list:
        data = self._get(f"/batches/{batch_id}/jobs", params={"limit": limit})
        return data.get("jobs", [])

    def requeue_stale(self, lease_seconds: int = 300):
        try:
            data = self._post("/jobs/requeue-stale", {"lease_seconds": lease_seconds})
            log.info("Requeued stale jobs: %s", data)
        except Exception as e:
            log.warning("requeue-stale failed (non-fatal): %s", e)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def init_db(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS verifier_jobs (
            record_id     INTEGER,
            email         TEXT,
            job_id        TEXT,
            batch_id      TEXT,
            status        TEXT DEFAULT 'submitted',
            result_status TEXT,
            result_message TEXT,
            submitted_at  TEXT,
            completed_at  TEXT,
            PRIMARY KEY (record_id, email)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vj_batch ON verifier_jobs(batch_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vj_record ON verifier_jobs(record_id)")
    conn.commit()
    log.info("verifier_jobs table ready")


def get_pending_record_ids(conn: sqlite3.Connection) -> list:
    """
    Return IDs of pending_validation records that still have at least one
    unfinished email in verifier_jobs (or no verifier_jobs entry at all).
    """
    rows = conn.execute("""
        SELECT r.id
        FROM records r
        WHERE r.status = 'pending_validation'
          AND r.id NOT IN (
              SELECT DISTINCT record_id
              FROM verifier_jobs
              WHERE status = 'done'
                AND record_id IN (
                    SELECT id FROM records WHERE status = 'pending_validation'
                )
              GROUP BY record_id
              HAVING COUNT(*) = (
                  SELECT COUNT(*)
                  FROM verifier_jobs vj2
                  WHERE vj2.record_id = record_id
              )
          )
    """).fetchall()
    # Simpler: just get all pending_validation that haven't been fully resolved
    rows = conn.execute("""
        SELECT id FROM records
        WHERE status = 'pending_validation'
    """).fetchall()
    # Exclude those already fully processed (all their verifier_jobs are done
    # AND the record status was already updated — but status is still
    # pending_validation only if we haven't updated it yet).
    return [r[0] for r in rows]


def get_unsubmitted_emails_for_records(conn: sqlite3.Connection, record_ids: list):
    """
    For each record_id, return emails not yet in verifier_jobs.
    Returns: dict[record_id] -> {email -> None}  (only unsubmitted emails)
    Also returns full record row for context.
    """
    placeholders = ",".join("?" * len(record_ids))
    rows = conn.execute(
        f"SELECT id, candidate_emails FROM records WHERE id IN ({placeholders})",
        record_ids,
    ).fetchall()

    # Already-submitted emails per record
    submitted = conn.execute(
        f"SELECT record_id, email FROM verifier_jobs WHERE record_id IN ({placeholders})",
        record_ids,
    ).fetchall()
    submitted_map = {}
    for rec_id, email in submitted:
        submitted_map.setdefault(rec_id, set()).add(email)

    result = {}  # record_id -> list[email]
    for rec_id, candidate_emails_raw in rows:
        if not candidate_emails_raw:
            continue
        try:
            emails = json.loads(candidate_emails_raw)
        except (json.JSONDecodeError, TypeError):
            emails = [candidate_emails_raw] if candidate_emails_raw else []
        already = submitted_map.get(rec_id, set())
        fresh = [e for e in emails if e and e not in already]
        if fresh:
            result[rec_id] = fresh
    return result


def insert_verifier_jobs(conn: sqlite3.Connection, record_id: int, email: str,
                          job_id: str, batch_id: str):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO verifier_jobs
           (record_id, email, job_id, batch_id, submitted_at)
           VALUES (?, ?, ?, ?, ?)""",
        (record_id, email, job_id, batch_id, now),
    )


def mark_verifier_job_done(conn: sqlite3.Connection, job_id: str,
                            result_status: str, result_message: str):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE verifier_jobs
           SET status='done', result_status=?, result_message=?, completed_at=?
           WHERE job_id=?""",
        (result_status, result_message, now, job_id),
    )


# ---------------------------------------------------------------------------
# Result selection logic
# ---------------------------------------------------------------------------
STATUS_PRIORITY = {"valid": 0, "catch_all": 1, "error": 2, "invalid": 3}

def pick_best_result(jobs: list) -> tuple:
    """
    Given a list of job dicts for one record, pick the best (email, status, score).
    Priority: valid > catch_all > error > invalid
    """
    best = None
    for job in jobs:
        st = job.get("result_status") or job.get("status")
        email = job.get("email", "")
        if best is None or STATUS_PRIORITY.get(st, 99) < STATUS_PRIORITY.get(best[1], 99):
            best = (email, st)
    if best is None:
        return None, "error", 0.0
    email, status = best
    score = {"valid": 1.0, "catch_all": 0.5}.get(status, 0.0)
    return email, status, score


def update_record(conn: sqlite3.Connection, record_id: int,
                  best_email: str, zuhal_status: str, zuhal_score: float):
    final_status = "validated" if zuhal_status in ("valid", "catch_all") else "validation_failed"
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE records
           SET status=?, candidate_email=?, zuhal_status=?, zuhal_score=?, updated_at=?
           WHERE id=?""",
        (final_status, best_email, zuhal_status, zuhal_score, now, record_id),
    )


# ---------------------------------------------------------------------------
# Batch polling
# ---------------------------------------------------------------------------
def poll_batch_until_done(client: EmailVerifierClient, batch_id: str) -> dict:
    """
    Poll GET /batches/{batch_id} until status == 'done' or timeout.
    Returns final status dict, or raises on timeout.
    """
    interval = POLL_START_S
    elapsed = 0
    last_counts = None
    while elapsed < POLL_TIMEOUT_S:
        data = client.get_batch_status(batch_id)
        status = data.get("status")
        counts = data.get("counts", {})
        done_count = data.get("done", 0)
        total = data.get("total", 0)

        if counts != last_counts:
            log.info(
                "Batch %s | %s | done=%d/%d | pending=%d processing=%d "
                "valid=%d invalid=%d catch_all=%d error=%d",
                batch_id, status, done_count, total,
                counts.get("pending", 0), counts.get("processing", 0),
                counts.get("valid", 0), counts.get("invalid", 0),
                counts.get("catch_all", 0), counts.get("error", 0),
            )
            last_counts = counts

        if status == "done":
            return data

        time.sleep(interval)
        elapsed += interval
        interval = min(interval * 2, POLL_MAX_S)

    log.warning("Batch %s timed out after %ds — requeueing stale jobs", batch_id, POLL_TIMEOUT_S)
    client.requeue_stale()
    raise TimeoutError(f"Batch {batch_id} did not complete within {POLL_TIMEOUT_S}s")


# ---------------------------------------------------------------------------
# Chunk processor
# ---------------------------------------------------------------------------
def process_chunk(conn: sqlite3.Connection, client: EmailVerifierClient,
                  record_ids: list, stats: dict):
    """
    Process one chunk of record IDs end-to-end:
    1. Find unsubmitted emails
    2. Submit in sub-batches
    3. Poll until done
    4. Save results to verifier_jobs + records
    """
    email_map = get_unsubmitted_emails_for_records(conn, record_ids)

    if not email_map:
        log.info("Chunk: all %d records already submitted — skipping", len(record_ids))
        return

    # Records with no emails at all
    no_email_ids = [rid for rid in record_ids if rid not in email_map]
    if no_email_ids:
        now = datetime.now(timezone.utc).isoformat()
        conn.executemany(
            "UPDATE records SET status='validation_failed', zuhal_status='error', updated_at=? WHERE id=?",
            [(now, rid) for rid in no_email_ids],
        )
        conn.commit()
        stats["validation_failed"] += len(no_email_ids)
        log.info("Marked %d records as validation_failed (no emails)", len(no_email_ids))

    # Flatten all emails with their record_id
    flat = []  # list of (record_id, email)
    for rec_id, emails in email_map.items():
        for email in emails:
            flat.append((rec_id, email))

    log.info("Chunk: %d records -> %d emails to submit", len(email_map), len(flat))

    # Submit in sub-batches
    for batch_start in range(0, len(flat), BATCH_SIZE):
        sub = flat[batch_start: batch_start + BATCH_SIZE]
        emails_only = [e for _, e in sub]

        try:
            batch_id, returned_jobs = client.submit_batch(emails_only)
        except TooManyFailuresError:
            raise
        except Exception as e:
            log.error("Failed to submit batch (offset %d): %s", batch_start, e)
            stats["api_errors"] += 1
            continue

        # Build job_id lookup: email -> job_id
        email_to_job = {j["email"]: j["id"] for j in returned_jobs}

        # Insert verifier_jobs rows
        for rec_id, email in sub:
            job_id = email_to_job.get(email, "")
            insert_verifier_jobs(conn, rec_id, email, job_id, batch_id)
        conn.commit()

        # For emails that were auto-marked as catch_all (returned in batch response)
        auto_catch_all = {j["email"]: j for j in returned_jobs if j.get("status") == "catch_all"}
        if auto_catch_all:
            for job in auto_catch_all.values():
                mark_verifier_job_done(conn, job["id"], "catch_all", job.get("message", "known catch-all domain"))
            conn.commit()

        # Poll for batch completion
        try:
            poll_batch_until_done(client, batch_id)
        except TimeoutError as e:
            log.error("%s", e)
            stats["timed_out_batches"] += 1
        except TooManyFailuresError:
            raise
        except Exception as e:
            log.error("Polling error for batch %s: %s", batch_id, e)
            stats["api_errors"] += 1
            continue

        # Fetch final job results
        try:
            batch_jobs = client.get_batch_jobs(batch_id, limit=len(sub) + 100)
        except Exception as e:
            log.error("Failed to fetch jobs for batch %s: %s", batch_id, e)
            stats["api_errors"] += 1
            continue

        # Update verifier_jobs with results
        for job in batch_jobs:
            mark_verifier_job_done(
                conn,
                job["id"],
                job.get("status", "error"),
                job.get("message") or "",
            )
        conn.commit()
        log.info("Batch %s results saved (%d jobs)", batch_id, len(batch_jobs))

    # Resolve each record: pick best result, update records table
    record_ids_with_emails = list(email_map.keys())
    placeholders = ",".join("?" * len(record_ids_with_emails))
    vj_rows = conn.execute(
        f"""SELECT record_id, email, result_status, result_message
            FROM verifier_jobs
            WHERE record_id IN ({placeholders}) AND status='done'""",
        record_ids_with_emails,
    ).fetchall()

    per_record = {}
    for rec_id, email, result_status, result_message in vj_rows:
        per_record.setdefault(rec_id, []).append({
            "email": email,
            "result_status": result_status,
            "result_message": result_message,
        })

    for rec_id, jobs in per_record.items():
        best_email, best_status, best_score = pick_best_result(jobs)
        update_record(conn, rec_id, best_email, best_status, best_score)
        if best_status == "valid":
            stats["valid"] += 1
        elif best_status == "catch_all":
            stats["catch_all"] += 1
        elif best_status == "invalid":
            stats["invalid"] += 1
        else:
            stats["error"] += 1
        if best_status in ("valid", "catch_all"):
            stats["validated"] += 1
        else:
            stats["validation_failed"] += 1

    # Records that had emails but never got a done verifier_job (timed out)
    resolved_ids = set(per_record.keys())
    unresolved = [rid for rid in record_ids_with_emails if rid not in resolved_ids]
    if unresolved:
        now = datetime.now(timezone.utc).isoformat()
        conn.executemany(
            "UPDATE records SET status='validation_failed', zuhal_status='error', updated_at=? WHERE id=?",
            [(now, rid) for rid in unresolved],
        )
        stats["validation_failed"] += len(unresolved)
        log.warning("%d records left unresolved (timeout) -> validation_failed", len(unresolved))

    conn.commit()


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------
def write_csv(conn: sqlite3.Connection, csv_path: str):
    log.info("Writing CSV to %s ...", csv_path)
    rows = conn.execute("""
        SELECT unique_id, business_name, agent_name, state,
               candidate_email, zuhal_status, zuhal_score
        FROM records
        WHERE status = 'validated'
        ORDER BY id
    """).fetchall()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["unique_id", "business_name", "agent_name", "state",
                         "email", "zuhal_status", "zuhal_score"])
        writer.writerows(rows)

    log.info("CSV written: %d validated records -> %s", len(rows), csv_path)
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Email Verification Pipeline")
    parser.add_argument("--db",  default=DB_PATH,  help="Path to pipeline.db")
    parser.add_argument("--out", default=CSV_PATH, help="Output CSV path")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Email Verification Pipeline starting")
    log.info("DB:  %s", args.db)
    log.info("CSV: %s", args.out)
    log.info("=" * 60)

    if not os.path.exists(args.db):
        log.critical("pipeline.db not found at %s", args.db)
        sys.exit(1)

    client = EmailVerifierClient(BASE_URL)

    # Step 1: health check
    client.health_check()

    # Step 2: open DB
    conn = sqlite3.connect(args.db, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")

    # Step 3: ensure verifier_jobs table exists
    init_db(conn)

    # Step 4: count work
    total_pending = conn.execute(
        "SELECT COUNT(*) FROM records WHERE status='pending_validation'"
    ).fetchone()[0]
    already_done = conn.execute(
        "SELECT COUNT(*) FROM records WHERE status IN ('validated','validation_failed')"
    ).fetchone()[0]
    log.info("Total pending_validation: %d | Already resolved: %d", total_pending, already_done)

    if total_pending == 0:
        log.info("Nothing to do — all records already processed.")
        write_csv(conn, args.out)
        conn.close()
        return

    # Step 5: iterate in chunks
    stats = {
        "chunks": 0,
        "validated": 0,
        "validation_failed": 0,
        "valid": 0,
        "catch_all": 0,
        "invalid": 0,
        "error": 0,
        "api_errors": 0,
        "timed_out_batches": 0,
    }

    run_start = time.time()
    offset = 0

    while True:
        record_ids = [
            r[0] for r in conn.execute(
                "SELECT id FROM records WHERE status='pending_validation' LIMIT ? OFFSET ?",
                (CHUNK_SIZE, offset),
            ).fetchall()
        ]
        if not record_ids:
            break

        stats["chunks"] += 1
        chunk_num = stats["chunks"]
        log.info("--- Chunk %d | records %d–%d ---", chunk_num, offset + 1, offset + len(record_ids))

        try:
            process_chunk(conn, client, record_ids, stats)
        except TooManyFailuresError as e:
            log.critical("ABORT: %s", e)
            log.critical("Writing partial CSV before exit...")
            write_csv(conn, args.out)
            conn.close()
            sys.exit(1)
        except KeyboardInterrupt:
            log.warning("Interrupted — writing partial CSV...")
            write_csv(conn, args.out)
            conn.close()
            sys.exit(0)
        except Exception as e:
            log.error("Unexpected error in chunk %d: %s", chunk_num, e, exc_info=True)
            # Continue with next chunk rather than crashing entirely

        # Progress log
        elapsed = time.time() - run_start
        resolved = stats["validated"] + stats["validation_failed"]
        pct = resolved / total_pending * 100 if total_pending else 0
        rate = resolved / elapsed * 60 if elapsed > 0 else 0
        log.info(
            "Progress: %d/%d (%.1f%%) | valid=%d catch_all=%d invalid=%d error=%d | %.0f rec/min",
            resolved, total_pending, pct,
            stats["valid"], stats["catch_all"], stats["invalid"], stats["error"],
            rate,
        )

        offset += CHUNK_SIZE

    # Step 6: write CSV
    csv_rows = write_csv(conn, args.out)
    conn.close()

    elapsed = time.time() - run_start
    log.info("DONE in %.1fs (%.1f min) | validated=%d failed=%d api_errors=%d",
             elapsed, elapsed / 60, stats["validated"], stats["validation_failed"], stats["api_errors"])


if __name__ == "__main__":
    main()
