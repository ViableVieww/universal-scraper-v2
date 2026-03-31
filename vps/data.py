"""Data transfer between local machine and VPS.

Usage::

    # Push input JSONL to VPS (renamed to current.jsonl by default)
    python -m vps.data push --file records/D1_D2_combined_part3.jsonl

    # Push with a custom remote filename
    python -m vps.data push --file records/tx_agents.jsonl --remote-name tx_agents.jsonl

    # Pull output, logs, and optionally pipeline.db from VPS
    python -m vps.data pull --output-dir output/run1
    python -m vps.data pull --output-dir output/run1 --include-db
    python -m vps.data pull --output-dir output/run1 --logs-only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import dotenv_values

from vps.ssh_client import SSHClient

WORK_DIR = "/root/usx"


def _load_env() -> dict[str, str]:
    env = dotenv_values(".env")
    required = ["VPS_IP", "VPS_USER", "VPS_PASS"]
    missing = [k for k in required if not env.get(k)]
    if missing:
        print(f"ERROR: Missing required .env keys: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return {k: v for k, v in env.items() if v}


def cmd_push(args: argparse.Namespace, env: dict[str, str]) -> None:
    local = Path(args.file)
    if not local.exists():
        print(f"ERROR: File not found: {local}", file=sys.stderr)
        sys.exit(1)

    remote_name = args.remote_name or "current.jsonl"
    remote_path = f"{WORK_DIR}/records/{remote_name}"
    size_mb = local.stat().st_size / (1024 * 1024)

    print(f"Pushing {local} ({size_mb:.1f} MB) → {env['VPS_IP']}:{remote_path}")

    with SSHClient(env["VPS_IP"], env["VPS_USER"], env["VPS_PASS"]) as ssh:
        # Ensure records dir exists
        ssh.exec(f"mkdir -p {WORK_DIR}/records")
        ssh.put(local, remote_path)

    print("Push complete.")


def cmd_pull(args: argparse.Namespace, env: dict[str, str]) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Pulling from {env['VPS_IP']} → {output_dir}/")

    with SSHClient(env["VPS_IP"], env["VPS_USER"], env["VPS_PASS"]) as ssh:
        if args.logs_only:
            count = ssh.get_dir(f"{WORK_DIR}/logs", output_dir / "logs")
            print(f"  logs/: {count} files")
            return

        # Pull output/
        if ssh.remote_exists(f"{WORK_DIR}/output"):
            count = ssh.get_dir(f"{WORK_DIR}/output", output_dir / "output")
            print(f"  output/: {count} files")
        else:
            print("  output/: (empty — no output yet)")

        # Pull logs/
        if ssh.remote_exists(f"{WORK_DIR}/logs"):
            count = ssh.get_dir(f"{WORK_DIR}/logs", output_dir / "logs")
            print(f"  logs/: {count} files")
        else:
            print("  logs/: (empty)")

        # Optionally pull pipeline.db
        if args.include_db:
            db_remote = f"{WORK_DIR}/pipeline.db"
            if ssh.remote_exists(db_remote):
                ssh.get(db_remote, output_dir / "pipeline.db")
                size_mb = (output_dir / "pipeline.db").stat().st_size / (1024 * 1024)
                print(f"  pipeline.db: {size_mb:.1f} MB")
            else:
                print("  pipeline.db: (not found on VPS)")

    print("Pull complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Transfer data to/from VPS")
    sub = parser.add_subparsers(dest="subcommand", required=True)

    push_p = sub.add_parser("push", help="Upload a JSONL file to the VPS")
    push_p.add_argument("--file", required=True, help="Local file path to push")
    push_p.add_argument("--remote-name", default="current.jsonl",
                        help="Filename on VPS (default: current.jsonl)")

    pull_p = sub.add_parser("pull", help="Download results and logs from the VPS")
    pull_p.add_argument("--output-dir", required=True, help="Local directory to write into")
    pull_p.add_argument("--include-db", action="store_true",
                        help="Also pull pipeline.db")
    pull_p.add_argument("--logs-only", action="store_true",
                        help="Pull only the logs/ directory")

    args = parser.parse_args()
    env = _load_env()

    if args.subcommand == "push":
        cmd_push(args, env)
    elif args.subcommand == "pull":
        cmd_pull(args, env)


if __name__ == "__main__":
    main()
