"""Remote pipeline status dashboard.

Usage::

    # One-shot status check
    python -m vps.status

    # Live refresh every 30 seconds
    python -m vps.status --watch 30

    # Pull pipeline.db snapshot and run local status query
    python -m vps.status --db-snapshot
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path

from dotenv import dotenv_values

from vps.ssh_client import SSHClient

WORK_DIR = "/root/usx"
LOG_TAIL_LINES = 20


def _load_env() -> dict[str, str]:
    env = dotenv_values(".env")
    required = ["VPS_IP", "VPS_USER", "VPS_PASS"]
    missing = [k for k in required if not env.get(k)]
    if missing:
        print(f"ERROR: Missing required .env keys: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return {k: v for k, v in env.items() if v}


def _print_status(env: dict[str, str], *, db_snapshot: bool) -> None:
    ip = env["VPS_IP"]

    with SSHClient(ip, env["VPS_USER"], env["VPS_PASS"]) as ssh:

        # ── systemctl status ──────────────────────────────────────────
        print(f"\n{'='*60}")
        print(f"  USX Pipeline Status  —  {ip}")
        print(f"{'='*60}\n")

        _, out, _ = ssh.exec(
            "systemctl status usx-producer usx-consumer "
            "--no-pager --lines=0 2>&1 || true"
        )
        print(out.strip())

        # ── pipeline status (via remote python -m pipeline status) ────
        print(f"\n{'─'*60}")
        print("  Pipeline Records\n")
        rc, out, err = ssh.exec(
            f"cd {WORK_DIR} && python3 -m pipeline status --db pipeline.db 2>&1"
        )
        if rc == 0 and out.strip():
            print(out.strip())
        elif err.strip():
            print(f"  (pipeline status error: {err.strip()})")
        else:
            print("  (no database yet)")

        # ── recent logs ───────────────────────────────────────────────
        for log_file, label in [
            (f"{WORK_DIR}/logs/producer-systemd.log", "Producer log (last 20 lines)"),
            (f"{WORK_DIR}/logs/consumer-systemd.log", "Consumer log (last 20 lines)"),
        ]:
            print(f"\n{'─'*60}")
            print(f"  {label}\n")
            rc, out, _ = ssh.exec(f"tail -n {LOG_TAIL_LINES} {log_file} 2>/dev/null || echo '(no log yet)'")
            print(out.strip())

        # ── optional db snapshot ──────────────────────────────────────
        if db_snapshot:
            print(f"\n{'─'*60}")
            print("  DB Snapshot (local)\n")
            db_remote = f"{WORK_DIR}/pipeline.db"
            if ssh.remote_exists(db_remote):
                with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
                    tmp_path = Path(tmp.name)
                ssh.get(db_remote, tmp_path)
                import subprocess
                result = subprocess.run(
                    ["python", "-m", "pipeline", "status", "--db", str(tmp_path)],
                    capture_output=True, text=True,
                )
                print(result.stdout.strip() or result.stderr.strip())
                tmp_path.unlink(missing_ok=True)
            else:
                print("  (pipeline.db not found on VPS)")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Remote VPS pipeline status")
    parser.add_argument("--watch", type=int, metavar="N",
                        help="Refresh every N seconds")
    parser.add_argument("--db-snapshot", action="store_true",
                        help="Pull pipeline.db and run local status query for richer output")
    args = parser.parse_args()

    env = _load_env()

    if args.watch:
        while True:
            try:
                _print_status(env, db_snapshot=args.db_snapshot)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
            except Exception as exc:
                print(f"\nStatus check failed: {exc}", file=sys.stderr)
            time.sleep(args.watch)
    else:
        _print_status(env, db_snapshot=args.db_snapshot)


if __name__ == "__main__":
    main()
