"""
check_status.py — Poll verification pipeline status on VPS
Usage:
    python check_status.py           # single check
    python check_status.py --watch   # refresh every 60s
    python check_status.py --watch --interval 30
"""

import argparse
import time
import sys

import paramiko

HOST   = ""
USER   = ""
PASSWD = ""
DB     = "/root/pipeline_clean.db"
LOG    = "/root/verify.log"

TOTAL_QUEUED = 186034


def connect():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(HOST, username=USER, password=PASSWD, timeout=20)
    c.get_transport().set_keepalive(15)
    return c


def run(client, cmd):
    _, out, err = client.exec_command(cmd)
    return out.read().decode("utf-8", errors="replace").strip()


def fetch_status(client):
    script = """python3 << 'EOF'
import sqlite3
c = sqlite3.connect('""" + DB + """')
statuses = dict(c.execute('SELECT status, COUNT(*) FROM records GROUP BY status').fetchall())
vj_done = c.execute('SELECT COUNT(*) FROM verifier_jobs WHERE status="done"').fetchone()[0]
vj_sub  = c.execute('SELECT COUNT(*) FROM verifier_jobs WHERE status="submitted"').fetchone()[0]
valid     = c.execute("SELECT COUNT(*) FROM records WHERE zuhal_status='valid'").fetchone()[0]
catch_all = c.execute("SELECT COUNT(*) FROM records WHERE zuhal_status='catch_all'").fetchone()[0]
invalid   = c.execute("SELECT COUNT(*) FROM records WHERE zuhal_status='invalid'").fetchone()[0]
error     = c.execute("SELECT COUNT(*) FROM records WHERE zuhal_status='error'").fetchone()[0]
for s,n in statuses.items(): print('STATUS', s, n)
print('VJ_DONE', vj_done)
print('VJ_SUB', vj_sub)
print('BREAKDOWN valid=' + str(valid) + ' catch_all=' + str(catch_all) + ' invalid=' + str(invalid) + ' error=' + str(error))
EOF"""
    db_stats = run(client, script)

    log_tail = run(client, f"tail -5 {LOG}")
    tmux_alive = run(client, "tmux ls 2>/dev/null | grep verify || echo stopped")
    return db_stats, log_tail, tmux_alive


def parse_stats(raw):
    statuses = {}
    breakdown = {}
    vj_done = vj_sub = 0
    for line in raw.splitlines():
        if line.startswith("STATUS "):
            _, k, v = line.split()
            statuses[k] = int(v)
        elif line.startswith("VJ_DONE"):
            vj_done = int(line.split()[1])
        elif line.startswith("VJ_SUB"):
            vj_sub = int(line.split()[1])
        elif line.startswith("BREAKDOWN"):
            for part in line.split()[1:]:
                if "=" in part:
                    bk, bv = part.split("=")
                    breakdown[bk] = int(bv)
    return statuses, vj_done, vj_sub, breakdown


def display(db_raw, log_tail, tmux_alive):
    statuses, vj_done, vj_sub, breakdown = parse_stats(db_raw)

    pending    = statuses.get("pending_validation", 0)
    validated  = statuses.get("validated", 0)
    val_failed = statuses.get("validation_failed", 0)
    processed  = validated + val_failed
    pct        = processed / TOTAL_QUEUED * 100

    remaining  = TOTAL_QUEUED - processed

    print("=" * 52)
    print(f"  Email Verification — Live Status")
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 52)
    print(f"  Total queued      {TOTAL_QUEUED:>10,}")
    print(f"  Processed         {processed:>10,}  ({pct:.1f}%)")
    print(f"  Remaining         {remaining:>10,}  ({100-pct:.1f}%)")
    print()
    print(f"  Validated         {validated:>10,}")
    print(f"    SMTP valid       {breakdown.get('valid',0):>9,}")
    print(f"    Accept-all       {breakdown.get('catch_all',0):>9,}")
    print(f"  Failed            {val_failed:>10,}")
    print(f"    Invalid          {breakdown.get('invalid',0):>9,}")
    print(f"    Error            {breakdown.get('error',0):>9,}")
    print()
    print(f"  verifier_jobs     done={vj_done:,}  submitted={vj_sub:,}")
    print()
    print(f"  Worker:  {tmux_alive}")
    print()
    print("  Last log lines:")
    for line in log_tail.splitlines():
        print(f"    {line}")
    print("=" * 52)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", action="store_true", help="Keep polling")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between polls (default 60)")
    args = parser.parse_args()

    while True:
        try:
            client = connect()
            db_raw, log_tail, tmux_alive = fetch_status(client)
            client.close()
            display(db_raw, log_tail, tmux_alive)
        except Exception as e:
            print(f"[ERROR] {e}", file=sys.stderr)

        if not args.watch:
            break

        print(f"\n  Refreshing in {args.interval}s... (Ctrl+C to stop)\n")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
