"""VPS deployment orchestrator.

Usage::

    # First-time provision
    python -m vps.deploy --provision \\
      --producer-flags "-i records/current.jsonl --chunk-size 100" \\
      --consumer-flags "--zuhal-rate-limit 150 --zuhal-concurrency 3"

    # Subsequent deploy (push code + restart)
    python -m vps.deploy

    # Push secrets only (e.g. after rotating an API key)
    python -m vps.deploy --secrets-only

    # Push code but do not restart running services
    python -m vps.deploy --skip-restart
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

from dotenv import dotenv_values

from vps.ssh_client import SSHClient

WORK_DIR = "/root/usx"

# systemd unit templates — ExecStart is injected dynamically
_PRODUCER_UNIT = """\
[Unit]
Description=USX Pipeline Producer Worker
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory={work_dir}
EnvironmentFile={work_dir}/.env
ExecStart=/usr/bin/python3 -m pipeline --producer-only {producer_flags}
Restart=on-failure
RestartSec=10
StandardOutput=append:{work_dir}/logs/producer-systemd.log
StandardError=append:{work_dir}/logs/producer-systemd.log

[Install]
WantedBy=multi-user.target
"""

_CONSUMER_UNIT = """\
[Unit]
Description=USX Pipeline Consumer Worker
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory={work_dir}
EnvironmentFile={work_dir}/.env
ExecStart=/usr/bin/python3 -m pipeline --consumer-only {consumer_flags}
Restart=on-failure
RestartSec=10
StandardOutput=append:{work_dir}/logs/consumer-systemd.log
StandardError=append:{work_dir}/logs/consumer-systemd.log

[Install]
WantedBy=multi-user.target
"""


def _load_env() -> dict[str, str]:
    env = dotenv_values(".env")
    required = ["VPS_IP", "VPS_USER", "VPS_PASS", "GH_TOKEN", "GH_REPO"]
    missing = [k for k in required if not env.get(k)]
    if missing:
        print(f"ERROR: Missing required .env keys: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return {k: v for k, v in env.items() if v}


def _run(ssh: SSHClient, cmd: str, *, label: str) -> str:
    print(f"  [{label}] $ {cmd}")
    rc, out, err = ssh.exec(cmd)
    if out.strip():
        print(out.rstrip())
    if err.strip():
        print(err.rstrip(), file=sys.stderr)
    if rc != 0:
        print(f"ERROR: '{label}' failed (exit {rc})", file=sys.stderr)
        sys.exit(rc)
    return out


def _get_existing_exec_start(ssh: SSHClient, unit: str) -> str:
    """Read the current ExecStart line from an installed unit, if it exists."""
    rc, out, _ = ssh.exec(f"grep '^ExecStart=' /etc/systemd/system/{unit} 2>/dev/null || true")
    line = out.strip()
    if line.startswith("ExecStart="):
        return line[len("ExecStart="):]
    return ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy USX pipeline to VPS")
    parser.add_argument("--provision", action="store_true",
                        help="First-time setup: create dirs, clone repo, install deps, write units, start services")
    parser.add_argument("--secrets-only", action="store_true",
                        help="Push .env to VPS and exit — no code pull, no restart")
    parser.add_argument("--skip-restart", action="store_true",
                        help="Deploy code and units but do not restart running services")
    parser.add_argument("--producer-flags", default="",
                        help="Flags to bake into the producer ExecStart line")
    parser.add_argument("--consumer-flags", default="",
                        help="Flags to bake into the consumer ExecStart line (--db and --log-dir always included)")
    args = parser.parse_args()

    env = _load_env()
    ip = env["VPS_IP"]
    user = env["VPS_USER"]
    password = env["VPS_PASS"]
    gh_token = env["GH_TOKEN"]
    gh_repo = env["GH_REPO"]

    print(f"\n=== USX Deploy → {ip} ===\n")

    with SSHClient(ip, user, password) as ssh:

        # ── Step 1: Push secrets ───────────────────────────────────────
        print("Step 1/5 — Pushing secrets")
        ssh.put(Path(".env"), f"{WORK_DIR}/.env")
        print("  .env uploaded")

        if args.secrets_only:
            print("\nDone (--secrets-only).")
            return

        # ── Step 2: Pull code ──────────────────────────────────────────
        print("\nStep 2/5 — Pulling code")
        repo_exists_rc, _, _ = ssh.exec(f"test -d {WORK_DIR}/.git")
        if repo_exists_rc == 0:
            _run(ssh, f"cd {WORK_DIR} && git pull origin main", label="git pull")
        else:
            _run(ssh,
                 f"git clone https://{gh_token}@github.com/{gh_repo}.git {WORK_DIR}",
                 label="git clone")

        if args.provision:
            _run(ssh, f"mkdir -p {WORK_DIR}/records {WORK_DIR}/output {WORK_DIR}/logs",
                 label="mkdir dirs")

        # ── Step 3: Install dependencies ──────────────────────────────
        print("\nStep 3/5 — Installing dependencies")
        _run(ssh,
             f"pip install -r {WORK_DIR}/requirements.txt --break-system-packages -q",
             label="pip install")

        # ── Step 4: Write systemd unit files ──────────────────────────
        print("\nStep 4/5 — Installing systemd units")

        # Determine ExecStart lines
        producer_cmd = args.producer_flags.strip()
        if not producer_cmd:
            # Preserve existing ExecStart if no flags given
            existing = _get_existing_exec_start(ssh, "usx-producer.service")
            if existing:
                # Strip the binary + module invocation prefix to get just the flags
                # e.g. "/usr/bin/python3 -m pipeline --producer-only <flags>"
                parts = existing.split("--producer-only", 1)
                producer_cmd = parts[1].strip() if len(parts) == 2 else ""

        consumer_extra = args.consumer_flags.strip()
        if not consumer_extra:
            existing = _get_existing_exec_start(ssh, "usx-consumer.service")
            if existing:
                parts = existing.split("--consumer-only", 1)
                consumer_extra = parts[1].strip() if len(parts) == 2 else ""

        # Always include --db and --log-dir defaults if not already present
        if "--db" not in consumer_extra:
            consumer_extra = f"--db {WORK_DIR}/pipeline.db {consumer_extra}".strip()
        if "--log-dir" not in consumer_extra:
            consumer_extra = f"--log-dir {WORK_DIR}/logs {consumer_extra}".strip()
        if "--db" not in producer_cmd:
            producer_cmd = f"--db {WORK_DIR}/pipeline.db {producer_cmd}".strip()
        if "--log-dir" not in producer_cmd:
            producer_cmd = f"--log-dir {WORK_DIR}/logs {producer_cmd}".strip()

        producer_unit = _PRODUCER_UNIT.format(
            work_dir=WORK_DIR,
            producer_flags=producer_cmd,
        )
        consumer_unit = _CONSUMER_UNIT.format(
            work_dir=WORK_DIR,
            consumer_flags=consumer_extra,
        )

        # Write units via SFTP (more reliable than heredoc over exec_command)
        for unit_name, content in [
            ("usx-producer.service", producer_unit),
            ("usx-consumer.service", consumer_unit),
        ]:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".service", delete=False) as tmp:
                tmp.write(content)
                tmp_path = Path(tmp.name)
            try:
                ssh.put(tmp_path, f"/etc/systemd/system/{unit_name}")
                print(f"  [write {unit_name}] uploaded via SFTP")
            finally:
                tmp_path.unlink(missing_ok=True)

        _run(ssh, "systemctl daemon-reload", label="daemon-reload")
        _run(ssh,
             "systemctl enable usx-producer usx-consumer 2>&1 || true",
             label="enable units")

        # ── Step 5: Restart services ───────────────────────────────────
        if args.skip_restart:
            print("\nStep 5/5 — Skipping restart (--skip-restart)")
        else:
            print("\nStep 5/5 — Starting/restarting services")
            _run(ssh, "systemctl restart usx-producer usx-consumer", label="restart")
            # Brief status check
            _, out, _ = ssh.exec(
                "systemctl is-active usx-producer usx-consumer 2>&1"
            )
            for svc, state in zip(
                ["usx-producer", "usx-consumer"], out.strip().splitlines()
            ):
                print(f"  {svc}: {state}")

    print("\nDeploy complete.\n")


if __name__ == "__main__":
    main()
