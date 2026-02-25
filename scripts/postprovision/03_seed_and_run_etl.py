#!/usr/bin/env python3
"""
03_seed_and_run_etl.py — Seed Confluence with sample data and trigger the ETL pipeline.

1. Seeds Confluence with sample spaces/pages/comments using the existing seeder.
2. Triggers the Data Factory pipeline run.
3. Waits for the pipeline to complete (with timeout).
"""

from __future__ import annotations

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env, FabricClient


def main() -> None:
    env = load_azd_env()
    workspace_id = require_env(env, "FABRIC_WORKSPACE_ID")
    pipeline_id = require_env(env, "FABRIC_PIPELINE_ID")
    confluence_url = env.get("CONFLUENCE_URL", "")
    confluence_email = env.get("CONFLUENCE_EMAIL", "")
    confluence_api_token = env.get("CONFLUENCE_API_TOKEN", "")

    # ── 1. Seed Confluence ─────────────────────────────────────────
    if confluence_url and confluence_email and confluence_api_token:
        print("  Seeding Confluence with sample data...")
        try:
            # Add project root to path
            project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..")
            )
            sys.path.insert(0, project_root)
            from src.confluence.seeder import ConfluenceSeeder

            seeder = ConfluenceSeeder(
                base_url=confluence_url,
                email=confluence_email,
                api_token=confluence_api_token,
            )
            result = seeder.seed_all()
            print(
                f"    Seeded {result.get('spaces_created', 0)} spaces, "
                f"{result.get('pages_created', 0)} pages, "
                f"{result.get('comments_created', 0)} comments"
            )
        except Exception as e:
            print(f"    [warn] Seeding failed (non-fatal): {e}")
            print("    Continuing with pipeline run...")
    else:
        print("  [skip] Confluence credentials not provided; skipping seed.")

    # ── 2. Trigger pipeline ────────────────────────────────────────
    print("  Triggering ETL pipeline run...")
    fabric = FabricClient()

    url = f"/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    resp = fabric.post(url)

    if resp.status_code not in (200, 202):
        print(f"    [warn] Pipeline trigger returned {resp.status_code}: {resp.text}")
        print("    You may need to run the pipeline manually from the Fabric UI.")
        return

    location = resp.headers.get("Location", "")
    job_id = location.rsplit("/", 1)[-1] if location else "unknown"
    print(f"    Pipeline triggered. Job ID: {job_id}")

    # ── 3. Wait for completion ─────────────────────────────────────
    print("  Waiting for pipeline to complete (timeout: 30 min)...")
    status_url = (
        f"/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
    )
    deadline = time.time() + 1800  # 30 minutes

    while time.time() < deadline:
        try:
            resp = fabric.get(status_url)
            data = resp.json()
            state = data.get("status", "Unknown")
            print(f"    Status: {state}")

            if state in ("Completed", "Failed", "Cancelled"):
                if state == "Failed":
                    reason = data.get("failureReason", "unknown")
                    print(f"    [warn] Pipeline failed: {reason}")
                elif state == "Completed":
                    print("    Pipeline completed successfully!")
                return
        except Exception as e:
            print(f"    [warn] Status check failed: {e}")

        time.sleep(30)

    print("    [warn] Timed out waiting for pipeline. Check Fabric UI for status.")


if __name__ == "__main__":
    main()
