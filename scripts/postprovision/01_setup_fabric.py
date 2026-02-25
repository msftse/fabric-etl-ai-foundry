#!/usr/bin/env python3
"""
01_setup_fabric.py — Create Fabric workspace and lakehouse.

Uses the Fabric REST API to:
  1. Create a workspace named 'confluence-etl'
  2. Assign the workspace to the provisioned Fabric capacity
  3. Create a lakehouse named 'confluencelakehouse'

Saves FABRIC_WORKSPACE_ID and FABRIC_LAKEHOUSE_ID back to the azd env.
"""

from __future__ import annotations

import subprocess
import sys
import os

# Add project root to path so we can import the helpers
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env, FabricClient


WORKSPACE_NAME = "confluence-etl"
LAKEHOUSE_NAME = "confluencelakehouse"


def set_azd_env(key: str, value: str) -> None:
    """Persist a value into the azd environment."""
    try:
        subprocess.run(
            ["azd", "env", "set", key, value],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    # Also set in current process for downstream scripts
    os.environ[key] = value


def resolve_capacity_guid(fabric: FabricClient, env: dict[str, str]) -> str:
    """
    Resolve the Fabric capacity GUID from available env vars.

    Bicep outputs FABRIC_CAPACITY_ID as a full ARM resource ID:
      /subscriptions/.../providers/Microsoft.Fabric/capacities/fcXXX
    But the Fabric REST API assignToCapacity expects a capacity GUID.

    Strategy:
      1. If FABRIC_CAPACITY_ID looks like a GUID already, use it directly.
      2. Otherwise, call GET /capacities to find the capacity by name
         (FABRIC_CAPACITY_NAME) and return its id (GUID).
    """
    import re

    capacity_id = env.get("FABRIC_CAPACITY_ID", "")
    capacity_name = env.get("FABRIC_CAPACITY_NAME", "")

    # Check if it's already a GUID (36 chars, 8-4-4-4-12 format)
    guid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
    )
    if guid_pattern.match(capacity_id):
        print(f"  Capacity ID is already a GUID: {capacity_id}")
        return capacity_id

    # It's a full ARM resource ID — resolve via Fabric API using the name
    if not capacity_name and "/" in capacity_id:
        # Extract the resource name from the ARM ID (last segment)
        capacity_name = capacity_id.rsplit("/", 1)[-1]

    if not capacity_name:
        print(
            "  [error] Cannot resolve capacity — no FABRIC_CAPACITY_NAME or valid FABRIC_CAPACITY_ID"
        )
        sys.exit(1)

    print(f"  Resolving capacity GUID for '{capacity_name}' via Fabric API...")
    resp = fabric.get("/capacities")
    capacities = resp.json().get("value", [])
    match = next(
        (
            c
            for c in capacities
            if c.get("displayName", "").lower() == capacity_name.lower()
        ),
        None,
    )
    if match:
        guid = match["id"]
        print(f"  Resolved capacity GUID: {guid}")
        return guid

    print(f"  [error] Capacity '{capacity_name}' not found in Fabric API response.")
    print(f"  Available capacities: {[c.get('displayName') for c in capacities]}")
    sys.exit(1)


def main() -> None:
    env = load_azd_env()
    fabric = FabricClient()

    # ── 0. Resolve capacity GUID ──────────────────────────────────
    capacity_guid = resolve_capacity_guid(fabric, env)

    # ── 1. Find or create workspace ────────────────────────────────
    print(f"  Looking for workspace '{WORKSPACE_NAME}'...")
    resp = fabric.get("/workspaces")
    workspaces = resp.json().get("value", [])
    workspace = next(
        (w for w in workspaces if w["displayName"] == WORKSPACE_NAME), None
    )

    if workspace:
        workspace_id = workspace["id"]
        print(f"  Workspace already exists: {workspace_id}")
    else:
        print(f"  Creating workspace '{WORKSPACE_NAME}'...")
        resp = fabric.post("/workspaces", {"displayName": WORKSPACE_NAME})
        resp.raise_for_status()
        workspace = resp.json()
        workspace_id = workspace["id"]
        print(f"  Workspace created: {workspace_id}")

    # ── 2. Assign capacity ─────────────────────────────────────────
    print(f"  Assigning workspace to capacity {capacity_guid}...")
    assign_resp = fabric.post(
        f"/workspaces/{workspace_id}/assignToCapacity",
        {"capacityId": capacity_guid},
    )
    if assign_resp.status_code in (200, 202):
        print("  Capacity assigned.")
    elif assign_resp.status_code == 409:
        print("  Workspace already assigned to a capacity.")
    else:
        print(
            f"  [warn] Capacity assignment returned {assign_resp.status_code}: {assign_resp.text}"
        )

    # ── 3. Find or create lakehouse ────────────────────────────────
    print(f"  Looking for lakehouse '{LAKEHOUSE_NAME}'...")
    resp = fabric.get(f"/workspaces/{workspace_id}/items?type=Lakehouse")
    lakehouses = resp.json().get("value", [])
    lakehouse = next(
        (lh for lh in lakehouses if lh["displayName"] == LAKEHOUSE_NAME), None
    )

    if lakehouse:
        lakehouse_id = lakehouse["id"]
        print(f"  Lakehouse already exists: {lakehouse_id}")
    else:
        print(f"  Creating lakehouse '{LAKEHOUSE_NAME}'...")
        resp = fabric.post(
            f"/workspaces/{workspace_id}/items",
            {"displayName": LAKEHOUSE_NAME, "type": "Lakehouse"},
        )
        resp.raise_for_status()

        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            if location:
                fabric.poll_lro(location)
            # Look up the lakehouse by name
            resp2 = fabric.get(f"/workspaces/{workspace_id}/items?type=Lakehouse")
            lakehouses = resp2.json().get("value", [])
            lakehouse = next(
                (lh for lh in lakehouses if lh["displayName"] == LAKEHOUSE_NAME),
                None,
            )
            lakehouse_id = lakehouse["id"] if lakehouse else ""
        else:
            lakehouse = resp.json()
            lakehouse_id = lakehouse["id"]
        print(f"  Lakehouse created: {lakehouse_id}")

    # ── 4. Persist IDs ─────────────────────────────────────────────
    set_azd_env("FABRIC_WORKSPACE_ID", workspace_id)
    set_azd_env("FABRIC_LAKEHOUSE_ID", lakehouse_id)
    set_azd_env("FABRIC_WORKSPACE_NAME", WORKSPACE_NAME)
    set_azd_env("FABRIC_LAKEHOUSE_NAME", LAKEHOUSE_NAME)

    print(f"  Done! workspace={workspace_id}, lakehouse={lakehouse_id}")


if __name__ == "__main__":
    main()
