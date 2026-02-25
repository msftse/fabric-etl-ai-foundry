"""
Shared utilities for post-provision scripts.

Loads environment variables set by `azd provision` (Bicep outputs)
and provides common helpers for Fabric REST API calls.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import requests
from azure.identity import DefaultAzureCredential


def load_azd_env() -> dict[str, str]:
    """
    Load environment variables from azd.

    Tries `azd env get-values` first; falls back to os.environ.
    Returns a dict of key=value pairs.
    """
    env = dict(os.environ)
    try:
        result = subprocess.run(
            ["azd", "env", "get-values"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().splitlines():
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, _, value = line.partition("=")
                    # Strip surrounding quotes
                    value = value.strip().strip('"').strip("'")
                    env[key.strip()] = value
    except (FileNotFoundError, subprocess.TimeoutExpired):
        print("  [warn] azd CLI not found or timed out; using os.environ only")
    return env


def require_env(env: dict[str, str], key: str) -> str:
    """Get a required env var or exit with an error."""
    value = env.get(key, "")
    if not value:
        print(f"  [error] Required environment variable {key} is not set.")
        sys.exit(1)
    return value


# ── Fabric REST API helpers ─────────────────────────────────────────

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


class FabricClient:
    """Lightweight Fabric REST API client for post-provision scripts."""

    def __init__(self) -> None:
        self._cred = DefaultAzureCredential()
        self._token: str | None = None
        self._token_expires: float = 0

    def _headers(self) -> dict[str, str]:
        now = time.time()
        if self._token is None or now >= self._token_expires - 60:
            tok = self._cred.get_token(FABRIC_SCOPE)
            self._token = tok.token
            self._token_expires = tok.expires_on
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def get(self, path: str) -> requests.Response:
        resp = requests.get(f"{FABRIC_API}{path}", headers=self._headers())
        resp.raise_for_status()
        return resp

    def post(self, path: str, body: dict | None = None) -> requests.Response:
        resp = requests.post(
            f"{FABRIC_API}{path}",
            headers=self._headers(),
            json=body or {},
        )
        return resp

    def poll_lro(self, location_url: str, timeout: int = 300) -> dict:
        """Poll a Fabric LRO until completion."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = requests.get(location_url, headers=self._headers())
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status", "Unknown")
            if status == "Succeeded":
                return data
            if status in ("Failed", "Cancelled"):
                raise RuntimeError(f"LRO failed: {data}")
            retry = int(resp.headers.get("Retry-After", 10))
            time.sleep(retry)
        raise TimeoutError("LRO timed out")
