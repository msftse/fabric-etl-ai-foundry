#!/usr/bin/env python3
"""
02_deploy_notebooks.py — Deploy 3 Fabric notebooks + Data Factory pipeline.

Reads the .ipynb files from notebooks/, injects workspace/lakehouse IDs,
uploads them via Fabric REST API, then creates the Data Factory pipeline
that chains Bronze -> Silver -> Gold.

Saves notebook and pipeline item IDs back to the azd env.
"""

from __future__ import annotations

import base64
import json
import subprocess
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env, FabricClient

# Project root for notebook files
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
NOTEBOOKS_DIR = os.path.join(PROJECT_ROOT, "notebooks")


def set_azd_env(key: str, value: str) -> None:
    try:
        subprocess.run(
            ["azd", "env", "set", key, value], capture_output=True, timeout=15
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    os.environ[key] = value


def load_notebook(filename: str, lakehouse_id: str, workspace_id: str) -> str:
    """Load a notebook .ipynb, replace placeholders, return base64-encoded content."""
    path = os.path.join(NOTEBOOKS_DIR, filename)
    with open(path, "r") as f:
        content = f.read()

    content = content.replace("{{LAKEHOUSE_ID}}", lakehouse_id)
    content = content.replace("{{WORKSPACE_ID}}", workspace_id)
    content = content.replace("{{LAKEHOUSE_NAME}}", "confluencelakehouse")

    return base64.b64encode(content.encode("utf-8")).decode("utf-8")


def deploy_notebook(
    fabric: FabricClient, workspace_id: str, name: str, b64_payload: str
) -> str:
    """Create or update a notebook. Returns the item ID."""
    definition = {
        "format": "ipynb",
        "parts": [
            {
                "path": "notebook-content.ipynb",
                "payload": b64_payload,
                "payloadType": "InlineBase64",
            }
        ],
    }

    # Check if exists
    resp = fabric.get(f"/workspaces/{workspace_id}/items?type=Notebook")
    notebooks = resp.json().get("value", [])
    existing = next((n for n in notebooks if n["displayName"] == name), None)

    if existing:
        item_id = existing["id"]
        print(f"    Updating notebook '{name}' ({item_id})...")
        import requests as req

        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/updateDefinition"
        resp = req.post(url, headers=fabric._headers(), json={"definition": definition})
        resp.raise_for_status()
        if resp.status_code == 202:
            loc = resp.headers.get("Location", "")
            if loc:
                fabric.poll_lro(loc)
    else:
        print(f"    Creating notebook '{name}'...")
        resp = fabric.post(
            f"/workspaces/{workspace_id}/items",
            {"displayName": name, "type": "Notebook", "definition": definition},
        )
        if resp.status_code == 202:
            loc = resp.headers.get("Location", "")
            if loc:
                fabric.poll_lro(loc)
            # Look up
            resp2 = fabric.get(f"/workspaces/{workspace_id}/items?type=Notebook")
            notebooks = resp2.json().get("value", [])
            existing = next((n for n in notebooks if n["displayName"] == name), None)
            item_id = existing["id"] if existing else ""
        else:
            resp.raise_for_status()
            item_id = resp.json()["id"]

    return item_id


def build_pipeline_json(
    bronze_id: str,
    silver_id: str,
    gold_id: str,
    workspace_id: str,
    confluence_url: str,
    confluence_email: str,
    confluence_api_token: str,
) -> str:
    """Build pipeline definition, return base64-encoded."""
    pipeline_def = {
        "properties": {
            "activities": [
                {
                    "name": "Bronze - Confluence Extract",
                    "type": "TridentNotebook",
                    "dependsOn": [],
                    "typeProperties": {
                        "notebookId": bronze_id,
                        "workspaceId": workspace_id,
                        "parameters": {
                            "confluence_url": {
                                "value": confluence_url,
                                "type": "string",
                            },
                            "confluence_email": {
                                "value": confluence_email,
                                "type": "string",
                            },
                            "confluence_api_token": {
                                "value": confluence_api_token,
                                "type": "string",
                            },
                        },
                    },
                    "policy": {
                        "timeout": "0.04:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureInput": True,
                        "secureOutput": False,
                    },
                },
                {
                    "name": "Silver - Cleanse and Transform",
                    "type": "TridentNotebook",
                    "dependsOn": [
                        {
                            "activity": "Bronze - Confluence Extract",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {
                        "notebookId": silver_id,
                        "workspaceId": workspace_id,
                    },
                    "policy": {
                        "timeout": "0.02:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureInput": False,
                        "secureOutput": False,
                    },
                },
                {
                    "name": "Gold - Aggregate",
                    "type": "TridentNotebook",
                    "dependsOn": [
                        {
                            "activity": "Silver - Cleanse and Transform",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {
                        "notebookId": gold_id,
                        "workspaceId": workspace_id,
                    },
                    "policy": {
                        "timeout": "0.02:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureInput": False,
                        "secureOutput": False,
                    },
                },
            ],
            "annotations": [],
        }
    }
    raw = json.dumps(pipeline_def)
    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


def deploy_pipeline(
    fabric: FabricClient, workspace_id: str, name: str, b64_payload: str
) -> str:
    """Create or update pipeline. Returns item ID."""
    definition = {
        "parts": [
            {
                "path": "pipeline-content.json",
                "payload": b64_payload,
                "payloadType": "InlineBase64",
            }
        ]
    }

    resp = fabric.get(f"/workspaces/{workspace_id}/items?type=DataPipeline")
    pipelines = resp.json().get("value", [])
    existing = next((p for p in pipelines if p["displayName"] == name), None)

    if existing:
        item_id = existing["id"]
        print(f"    Updating pipeline '{name}' ({item_id})...")
        import requests as req

        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/updateDefinition"
        resp = req.post(url, headers=fabric._headers(), json={"definition": definition})
        resp.raise_for_status()
        if resp.status_code == 202:
            loc = resp.headers.get("Location", "")
            if loc:
                fabric.poll_lro(loc)
    else:
        print(f"    Creating pipeline '{name}'...")
        resp = fabric.post(
            f"/workspaces/{workspace_id}/items",
            {"displayName": name, "type": "DataPipeline", "definition": definition},
        )
        if resp.status_code == 202:
            loc = resp.headers.get("Location", "")
            if loc:
                fabric.poll_lro(loc)
            resp2 = fabric.get(f"/workspaces/{workspace_id}/items?type=DataPipeline")
            pipelines = resp2.json().get("value", [])
            existing = next((p for p in pipelines if p["displayName"] == name), None)
            item_id = existing["id"] if existing else ""
        else:
            resp.raise_for_status()
            item_id = resp.json()["id"]

    return item_id


def main() -> None:
    env = load_azd_env()
    workspace_id = require_env(env, "FABRIC_WORKSPACE_ID")
    lakehouse_id = require_env(env, "FABRIC_LAKEHOUSE_ID")
    confluence_url = env.get("CONFLUENCE_URL", "")
    confluence_email = env.get("CONFLUENCE_EMAIL", "")
    confluence_api_token = env.get("CONFLUENCE_API_TOKEN", "")

    fabric = FabricClient()

    # Deploy notebooks
    print("  Deploying Bronze notebook...")
    bronze_b64 = load_notebook(
        "bronze_confluence_extract.ipynb", lakehouse_id, workspace_id
    )
    bronze_id = deploy_notebook(
        fabric, workspace_id, "Bronze - Confluence Extract", bronze_b64
    )
    print(f"    Bronze notebook ID: {bronze_id}")

    print("  Deploying Silver notebook...")
    silver_b64 = load_notebook(
        "silver_confluence_transform.ipynb", lakehouse_id, workspace_id
    )
    silver_id = deploy_notebook(
        fabric, workspace_id, "Silver - Cleanse and Transform", silver_b64
    )
    print(f"    Silver notebook ID: {silver_id}")

    print("  Deploying Gold notebook...")
    gold_b64 = load_notebook(
        "gold_confluence_aggregation.ipynb", lakehouse_id, workspace_id
    )
    gold_id = deploy_notebook(fabric, workspace_id, "Gold - Aggregate", gold_b64)
    print(f"    Gold notebook ID: {gold_id}")

    # Deploy pipeline
    print("  Deploying ETL pipeline...")
    pipeline_b64 = build_pipeline_json(
        bronze_id,
        silver_id,
        gold_id,
        workspace_id,
        confluence_url,
        confluence_email,
        confluence_api_token,
    )
    pipeline_id = deploy_pipeline(fabric, workspace_id, "ConfluenceETL", pipeline_b64)
    print(f"    Pipeline ID: {pipeline_id}")

    # Persist
    set_azd_env("FABRIC_BRONZE_NOTEBOOK_ID", bronze_id)
    set_azd_env("FABRIC_SILVER_NOTEBOOK_ID", silver_id)
    set_azd_env("FABRIC_GOLD_NOTEBOOK_ID", gold_id)
    set_azd_env("FABRIC_PIPELINE_ID", pipeline_id)

    print("  Done! Notebooks and pipeline deployed.")


if __name__ == "__main__":
    main()
