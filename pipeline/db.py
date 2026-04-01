from __future__ import annotations

from pathlib import Path

import aiosqlite

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS records (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    unique_id           TEXT NOT NULL UNIQUE,
    business_name       TEXT,
    agent_name          TEXT,
    state               TEXT,
    jurisdiction        TEXT,
    position_type       TEXT,
    name_entity_type    TEXT,

    -- Discovery outputs
    candidate_email     TEXT,
    candidate_emails    TEXT,
    candidate_domain    TEXT,
    discovery_source    TEXT,
    discovery_attempts  INTEGER DEFAULT 0,
    strategy            TEXT,
    is_org_agent        INTEGER DEFAULT 0,

    -- Validation outputs
    zuhal_status        TEXT,
    zuhal_score         REAL,
    validation_attempts INTEGER DEFAULT 0,

    -- State machine
    status              TEXT NOT NULL DEFAULT 'queued',

    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS checkpoints (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stats (
    run_id              TEXT PRIMARY KEY,
    total_input         INTEGER DEFAULT 0,
    producer_processed  INTEGER DEFAULT 0,
    discovery_hits      INTEGER DEFAULT 0,
    discovery_misses    INTEGER DEFAULT 0,
    validated           INTEGER DEFAULT 0,
    validation_failed   INTEGER DEFAULT 0,
    serper_calls        INTEGER DEFAULT 0,
    brave_calls         INTEGER DEFAULT 0,
    zuhal_calls         INTEGER DEFAULT 0,
    estimated_cost_usd  REAL DEFAULT 0.0,
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS failures (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    unique_id       TEXT NOT NULL,
    phase           TEXT NOT NULL,
    attempt         INTEGER NOT NULL,
    error_type      TEXT,
    error_detail    TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_records_status ON records(status);
CREATE INDEX IF NOT EXISTS idx_records_unique_id ON records(unique_id);
CREATE INDEX IF NOT EXISTS idx_failures_unique_id ON failures(unique_id);
"""

INSERT_RECORD_SQL = """
INSERT OR IGNORE INTO records (
    unique_id, business_name, agent_name, state, jurisdiction,
    position_type, name_entity_type, candidate_email, candidate_emails,
    candidate_domain, discovery_source, discovery_attempts,
    strategy, is_org_agent, status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_CHECKPOINT_SQL = """
INSERT INTO checkpoints (key, value, updated_at)
VALUES (?, ?, datetime('now'))
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')
"""


async def init_db(db_path: Path) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(str(db_path))
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.executescript(SCHEMA_SQL)
    await conn.commit()
    conn.row_factory = aiosqlite.Row
    return conn


async def get_checkpoint(conn: aiosqlite.Connection, key: str) -> str | None:
    async with conn.execute(
        "SELECT value FROM checkpoints WHERE key = ?", (key,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None


async def upsert_checkpoint(conn: aiosqlite.Connection, key: str, value: str) -> None:
    await conn.execute(UPSERT_CHECKPOINT_SQL, (key, value))


async def insert_records_batch(
    conn: aiosqlite.Connection,
    records: list[dict],
    new_offset: int,
) -> None:
    """Atomically insert a batch of records and advance the producer checkpoint."""
    async with conn.cursor() as cur:
        await cur.execute("BEGIN")
        try:
            for r in records:
                await cur.execute(INSERT_RECORD_SQL, (
                    r["unique_id"],
                    r.get("business_name", ""),
                    r.get("agent_name", ""),
                    r.get("state", ""),
                    r.get("jurisdiction", ""),
                    r.get("position_type", ""),
                    r.get("name_entity_type", ""),
                    r.get("candidate_email"),
                    r.get("candidate_emails"),
                    r.get("candidate_domain"),
                    r.get("discovery_source"),
                    r.get("discovery_attempts", 0),
                    r.get("strategy"),
                    1 if r.get("is_org_agent") else 0,
                    r.get("status", "queued"),
                ))
            await cur.execute(UPSERT_CHECKPOINT_SQL, ("producer_offset", str(new_offset)))
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


async def fetch_pending_validation(
    conn: aiosqlite.Connection,
    limit: int = 10,
) -> list[aiosqlite.Row]:
    async with conn.execute(
        "SELECT * FROM records WHERE status = 'pending_validation' LIMIT ?",
        (limit,),
    ) as cursor:
        return await cursor.fetchall()


async def fetch_pending_discovery(
    conn: aiosqlite.Connection,
    limit: int = 100,
) -> list[aiosqlite.Row]:
    async with conn.execute(
        "SELECT * FROM records WHERE status = 'pending_discovery' LIMIT ?",
        (limit,),
    ) as cursor:
        return await cursor.fetchall()


async def update_record_discovery(conn: aiosqlite.Connection, result: dict) -> None:
    """UPDATE discovery fields on an existing record (used by the retry loop)."""
    await conn.execute(
        """UPDATE records SET
               status = ?, candidate_email = ?, candidate_emails = ?,
               candidate_domain = ?, discovery_source = ?, discovery_attempts = ?,
               updated_at = datetime('now')
           WHERE unique_id = ?""",
        (
            result.get("status", "pending_discovery"),
            result.get("candidate_email"),
            result.get("candidate_emails"),
            result.get("candidate_domain"),
            result.get("discovery_source"),
            result.get("discovery_attempts", 1),
            result["unique_id"],
        ),
    )
    await conn.commit()


async def update_record_status(
    conn: aiosqlite.Connection,
    unique_id: str,
    status: str,
    **extra_fields: object,
) -> None:
    sets = ["status = ?", "updated_at = datetime('now')"]
    values: list[object] = [status]

    for col, val in extra_fields.items():
        sets.append(f"{col} = ?")
        values.append(val)

    values.append(unique_id)

    sql = f"UPDATE records SET {', '.join(sets)} WHERE unique_id = ?"
    await conn.execute(sql, values)
    await conn.commit()


async def insert_failure(
    conn: aiosqlite.Connection,
    unique_id: str,
    phase: str,
    attempt: int,
    error_type: str,
    error_detail: str,
) -> None:
    await conn.execute(
        "INSERT INTO failures (unique_id, phase, attempt, error_type, error_detail) "
        "VALUES (?, ?, ?, ?, ?)",
        (unique_id, phase, attempt, error_type, error_detail),
    )
    await conn.commit()


async def upsert_stats(
    conn: aiosqlite.Connection,
    run_id: str,
    **fields: object,
) -> None:
    # Build a dynamic upsert
    cols = ["run_id"]
    vals: list[object] = [run_id]
    updates = ["updated_at = datetime('now')"]

    for col, val in fields.items():
        cols.append(col)
        vals.append(val)
        updates.append(f"{col} = excluded.{col}")

    placeholders = ", ".join(["?"] * len(cols))
    col_str = ", ".join(cols)
    update_str = ", ".join(updates)

    sql = (
        f"INSERT INTO stats ({col_str}) VALUES ({placeholders}) "
        f"ON CONFLICT(run_id) DO UPDATE SET {update_str}"
    )
    await conn.execute(sql, vals)
    await conn.commit()


async def get_status_summary(conn: aiosqlite.Connection) -> dict:
    summary: dict = {}

    # Record counts by status
    async with conn.execute(
        "SELECT status, COUNT(*) FROM records GROUP BY status"
    ) as cursor:
        summary["records_by_status"] = {row[0]: row[1] async for row in cursor}

    # Total records
    async with conn.execute("SELECT COUNT(*) FROM records") as cursor:
        row = await cursor.fetchone()
        summary["total_records"] = row[0] if row else 0

    # Producer offset
    offset = await get_checkpoint(conn, "producer_offset")
    summary["producer_offset"] = int(offset) if offset else 0

    # Producer done?
    done = await get_checkpoint(conn, "producer_done")
    summary["producer_done"] = done == "true"

    # Stats
    async with conn.execute("SELECT * FROM stats LIMIT 1") as cursor:
        row = await cursor.fetchone()
        if row:
            summary["stats"] = dict(row)

    # Failure counts by phase
    async with conn.execute(
        "SELECT phase, COUNT(*) FROM failures GROUP BY phase"
    ) as cursor:
        summary["failures_by_phase"] = {row[0]: row[1] async for row in cursor}

    return summary


async def reset_failed_records(
    conn: aiosqlite.Connection,
    status: str = "discovery_failed",
    phase: str | None = None,
) -> int:
    if phase:
        # Only reset records that have failures in the specified phase
        sql = """
            UPDATE records SET status = 'queued', discovery_attempts = 0, updated_at = datetime('now')
            WHERE status = ? AND unique_id IN (
                SELECT DISTINCT unique_id FROM failures WHERE phase = ?
            )
        """
        cursor = await conn.execute(sql, (status, phase))
    else:
        if status == "validation_failed":
            sql = """
                UPDATE records SET status = 'pending_validation', validation_attempts = 0,
                updated_at = datetime('now')
                WHERE status = ?
            """
        else:
            sql = """
                UPDATE records SET status = 'queued', discovery_attempts = 0,
                updated_at = datetime('now')
                WHERE status = ?
            """
        cursor = await conn.execute(sql, (status,))

    await conn.commit()
    return cursor.rowcount
