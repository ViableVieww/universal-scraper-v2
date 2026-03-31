from __future__ import annotations

import io
import stat
from pathlib import Path
from typing import Generator

import paramiko


class SSHClient:
    """Thin paramiko wrapper providing exec, put, get, and get_text operations."""

    def __init__(self, host: str, user: str, password: str) -> None:
        self._host = host
        self._user = user
        self._password = password
        self._client: paramiko.SSHClient | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> SSHClient:
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.connect(
            self._host,
            username=self._user,
            password=self._password,
            timeout=30,
        )
        return self

    def __exit__(self, *_: object) -> None:
        if self._client:
            self._client.close()
            self._client = None

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    def exec(self, cmd: str) -> tuple[int, str, str]:
        """Run *cmd* on the remote host.

        Returns (returncode, stdout, stderr).
        """
        assert self._client, "Not connected — use as context manager"
        _, stdout, stderr = self._client.exec_command(cmd)
        out = stdout.read().decode()
        err = stderr.read().decode()
        rc = stdout.channel.recv_exit_status()
        return rc, out, err

    def put(self, local: Path, remote: str) -> None:
        """Upload *local* file to *remote* path on the VPS."""
        assert self._client, "Not connected"
        with self._client.open_sftp() as sftp:
            sftp.put(str(local), remote)

    def get(self, remote: str, local: Path) -> None:
        """Download *remote* path from the VPS to *local*."""
        assert self._client, "Not connected"
        local.parent.mkdir(parents=True, exist_ok=True)
        with self._client.open_sftp() as sftp:
            sftp.get(remote, str(local))

    def get_text(self, remote: str) -> str:
        """Read a text file from the VPS and return its contents."""
        assert self._client, "Not connected"
        with self._client.open_sftp() as sftp:
            with sftp.open(remote, "r") as fh:
                return fh.read().decode()

    def get_dir(self, remote_dir: str, local_dir: Path) -> int:
        """Recursively download *remote_dir* into *local_dir*.

        Returns the number of files downloaded.
        """
        assert self._client, "Not connected"
        local_dir.mkdir(parents=True, exist_ok=True)
        count = 0
        with self._client.open_sftp() as sftp:
            count = _sftp_get_dir(sftp, remote_dir, local_dir)
        return count

    def remote_exists(self, remote: str) -> bool:
        """Return True if *remote* path exists on the VPS."""
        assert self._client, "Not connected"
        with self._client.open_sftp() as sftp:
            try:
                sftp.stat(remote)
                return True
            except FileNotFoundError:
                return False


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _sftp_get_dir(sftp: paramiko.SFTPClient, remote: str, local: Path) -> int:
    count = 0
    for entry in sftp.listdir_attr(remote):
        remote_path = f"{remote}/{entry.filename}"
        local_path = local / entry.filename
        if stat.S_ISDIR(entry.st_mode or 0):
            local_path.mkdir(parents=True, exist_ok=True)
            count += _sftp_get_dir(sftp, remote_path, local_path)
        else:
            sftp.get(remote_path, str(local_path))
            count += 1
    return count
