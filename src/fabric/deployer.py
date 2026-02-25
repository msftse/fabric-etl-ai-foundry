"""
Fabric Data Factory Deployer.

Deploys Fabric Notebooks and a Data Factory pipeline to a Fabric workspace
using the Fabric REST API.  The pipeline chains three Notebook activities
(Bronze -> Silver -> Gold) to run the full Confluence ETL inside Fabric.

Authentication uses ``DefaultAzureCredential`` (az cli, managed identity, etc.)
to obtain a bearer token for ``https://api.fabric.microsoft.com``.
"""

from __future__ import annotations

import json
import time
import base64
from dataclasses import dataclass

import requests
from azure.identity import DefaultAzureCredential

from src.utils.logging import get_logger

log = get_logger(__name__)

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


# ── Dataclass for deployed item references ──────────────────────────


@dataclass
class DeployedItem:
    item_id: str
    display_name: str
    item_type: str


# ── Deployer ────────────────────────────────────────────────────────


class FabricDeployer:
    """Create / update Fabric Notebooks + Data Factory pipeline via REST API."""

    def __init__(self, workspace_id: str, lakehouse_id: str) -> None:
        self._ws = workspace_id
        self._lh = lakehouse_id
        self._cred = DefaultAzureCredential()
        self._token: str | None = None
        self._token_expires: float = 0

    # ── Auth ─────────────────────────────────────────────────────────

    def _headers(self) -> dict[str, str]:
        now = time.time()
        if self._token is None or now >= self._token_expires - 60:
            tok = self._cred.get_token(FABRIC_SCOPE)
            self._token = tok.token
            self._token_expires = tok.expires_on
            log.info("fabric_token_acquired")
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    # ── Low-level helpers ────────────────────────────────────────────

    def _items_url(self, suffix: str = "") -> str:
        return f"{FABRIC_API}/workspaces/{self._ws}/items{suffix}"

    def _list_items(self, item_type: str | None = None) -> list[dict]:
        """List all items in the workspace, optionally filtered by type."""
        url = self._items_url()
        if item_type:
            url += f"?type={item_type}"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        return resp.json().get("value", [])

    def _find_item(self, display_name: str, item_type: str) -> dict | None:
        """Find an existing item by display name and type."""
        for item in self._list_items(item_type):
            if item.get("displayName") == display_name:
                return item
        return None

    def _poll_lro(
        self, location_url: str, poll_seconds: int = 10, timeout_seconds: int = 300
    ) -> dict:
        """
        Poll a Fabric long-running operation (LRO) until it completes.

        Returns the final operation response dict.
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            resp = requests.get(location_url, headers=self._headers())
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status", "Unknown")
            log.info("lro_poll", status=status)
            if status == "Succeeded":
                return data
            if status in ("Failed", "Cancelled"):
                error = data.get("error", {})
                raise RuntimeError(
                    f"LRO failed: {error.get('errorCode', 'Unknown')} — "
                    f"{error.get('message', 'no details')}"
                )
            retry_after = int(resp.headers.get("Retry-After", poll_seconds))
            time.sleep(retry_after)
        raise TimeoutError(f"LRO timed out after {timeout_seconds}s")

    def _create_item(
        self, display_name: str, item_type: str, definition: dict | None = None
    ) -> dict:
        """
        Create a new item in the workspace.

        Handles both synchronous (201) and asynchronous/LRO (202) responses.
        For LRO, polls the operation URL and then looks up the item by name.
        """
        payload: dict = {
            "displayName": display_name,
            "type": item_type,
        }
        if definition:
            payload["definition"] = definition
        resp = requests.post(self._items_url(), headers=self._headers(), json=payload)
        resp.raise_for_status()

        # Synchronous creation — item returned directly
        if resp.status_code == 201:
            return resp.json()

        # Asynchronous (LRO) — poll until completion, then find the item
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            if location:
                self._poll_lro(location)
            # After LRO completes, look up the item by name
            item = self._find_item(display_name, item_type)
            if item:
                return item
            raise RuntimeError(
                f"LRO completed but could not find item '{display_name}' "
                f"of type '{item_type}' in the workspace"
            )

        # Unexpected status code
        return resp.json()

    def _update_definition(self, item_id: str, definition: dict) -> None:
        """Update the definition (content) of an existing item.  Handles LRO."""
        url = self._items_url(f"/{item_id}/updateDefinition")
        resp = requests.post(
            url, headers=self._headers(), json={"definition": definition}
        )
        resp.raise_for_status()
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            if location:
                self._poll_lro(location)
        log.info("item_definition_updated", item_id=item_id)

    # ── Notebook deployment ──────────────────────────────────────────

    def deploy_notebook(self, display_name: str, base64_payload: str) -> DeployedItem:
        """
        Create or update a Fabric Notebook.

        ``base64_payload`` is the base64-encoded .ipynb JSON string, as returned
        by the functions in ``src.fabric.notebook_content``.
        """
        definition = {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": base64_payload,
                    "payloadType": "InlineBase64",
                }
            ],
        }

        existing = self._find_item(display_name, "Notebook")
        if existing:
            item_id = existing["id"]
            log.info("notebook_exists_updating", name=display_name, id=item_id)
            self._update_definition(item_id, definition)
        else:
            log.info("creating_notebook", name=display_name)
            result = self._create_item(display_name, "Notebook", definition)
            item_id = result["id"]
            log.info("notebook_created", name=display_name, id=item_id)

        return DeployedItem(
            item_id=item_id, display_name=display_name, item_type="Notebook"
        )

    # ── Pipeline definition builder ─────────────────────────────────

    @staticmethod
    def _build_pipeline_json(
        bronze_notebook_id: str,
        silver_notebook_id: str,
        gold_notebook_id: str,
        workspace_id: str,
        confluence_url: str = "",
        confluence_email: str = "",
        confluence_api_token: str = "",
    ) -> str:
        """
        Build the pipeline-content.json for a 3-stage Notebook ETL pipeline.

        Returns the base64-encoded JSON string.
        """
        pipeline_def = {
            "properties": {
                "activities": [
                    {
                        "name": "Bronze - Confluence Extract",
                        "type": "TridentNotebook",
                        "dependsOn": [],
                        "typeProperties": {
                            "notebookId": bronze_notebook_id,
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
                            "notebookId": silver_notebook_id,
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
                            "notebookId": gold_notebook_id,
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

    # ── Pipeline deployment ──────────────────────────────────────────

    def deploy_pipeline(
        self,
        pipeline_name: str,
        bronze_notebook_id: str,
        silver_notebook_id: str,
        gold_notebook_id: str,
        confluence_url: str = "",
        confluence_email: str = "",
        confluence_api_token: str = "",
    ) -> DeployedItem:
        """Create or update the Data Factory pipeline."""
        b64_payload = self._build_pipeline_json(
            bronze_notebook_id=bronze_notebook_id,
            silver_notebook_id=silver_notebook_id,
            gold_notebook_id=gold_notebook_id,
            workspace_id=self._ws,
            confluence_url=confluence_url,
            confluence_email=confluence_email,
            confluence_api_token=confluence_api_token,
        )

        definition = {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": b64_payload,
                    "payloadType": "InlineBase64",
                }
            ]
        }

        existing = self._find_item(pipeline_name, "DataPipeline")
        if existing:
            item_id = existing["id"]
            log.info("pipeline_exists_updating", name=pipeline_name, id=item_id)
            self._update_definition(item_id, definition)
        else:
            log.info("creating_pipeline", name=pipeline_name)
            result = self._create_item(pipeline_name, "DataPipeline", definition)
            item_id = result["id"]
            log.info("pipeline_created", name=pipeline_name, id=item_id)

        return DeployedItem(
            item_id=item_id, display_name=pipeline_name, item_type="DataPipeline"
        )

    # ── Full deploy (notebooks + pipeline) ───────────────────────────

    def deploy_all(
        self,
        confluence_url: str = "",
        confluence_email: str = "",
        confluence_api_token: str = "",
        pipeline_name: str = "ConfluenceETL",
    ) -> dict[str, DeployedItem]:
        """
        Deploy all three notebooks and the orchestration pipeline.

        Returns a dict of deployed items keyed by role name.
        """
        from src.fabric.notebook_content import (
            bronze_notebook,
            silver_notebook,
            gold_notebook,
        )

        log.info("deploy_all_start")

        # 1. Deploy notebooks
        bronze = self.deploy_notebook(
            "Bronze - Confluence Extract",
            bronze_notebook(self._lh, self._ws),
        )
        silver = self.deploy_notebook(
            "Silver - Cleanse and Transform",
            silver_notebook(self._lh, self._ws),
        )
        gold = self.deploy_notebook(
            "Gold - Aggregate",
            gold_notebook(self._lh, self._ws),
        )

        # 2. Deploy pipeline referencing the notebooks
        pipeline = self.deploy_pipeline(
            pipeline_name=pipeline_name,
            bronze_notebook_id=bronze.item_id,
            silver_notebook_id=silver.item_id,
            gold_notebook_id=gold.item_id,
            confluence_url=confluence_url,
            confluence_email=confluence_email,
            confluence_api_token=confluence_api_token,
        )

        log.info(
            "deploy_all_done",
            bronze_id=bronze.item_id,
            silver_id=silver.item_id,
            gold_id=gold.item_id,
            pipeline_id=pipeline.item_id,
        )

        return {
            "bronze_notebook": bronze,
            "silver_notebook": silver,
            "gold_notebook": gold,
            "pipeline": pipeline,
        }

    # ── Run pipeline ─────────────────────────────────────────────────

    def run_pipeline(self, pipeline_item_id: str) -> str:
        """
        Trigger an on-demand run of the pipeline.

        Returns the job instance ID (from the Location header).
        """
        url = self._items_url(f"/{pipeline_item_id}/jobs/instances?jobType=Pipeline")
        resp = requests.post(url, headers=self._headers())
        resp.raise_for_status()

        # The job instance ID is in the Location header
        location = resp.headers.get("Location", "")
        job_instance_id = location.rsplit("/", 1)[-1] if location else ""
        log.info(
            "pipeline_run_triggered",
            pipeline_id=pipeline_item_id,
            job_instance_id=job_instance_id,
        )
        return job_instance_id

    # ── Get pipeline run status ──────────────────────────────────────

    def get_run_status(self, item_id: str, job_instance_id: str) -> dict:
        """
        Get the status of a pipeline job instance.

        Returns the full status dict including ``status``, ``startTimeUtc``,
        ``endTimeUtc``, ``failureReason``, etc.
        """
        url = self._items_url(f"/{item_id}/jobs/instances/{job_instance_id}")
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def wait_for_completion(
        self,
        item_id: str,
        job_instance_id: str,
        poll_seconds: int = 30,
        timeout_seconds: int = 7200,
    ) -> dict:
        """
        Poll until the pipeline run completes (or times out).

        Returns the final status dict.
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            status = self.get_run_status(item_id, job_instance_id)
            state = status.get("status", "Unknown")
            log.info("pipeline_run_status", state=state, job_id=job_instance_id)
            if state in ("Completed", "Failed", "Cancelled"):
                return status
            time.sleep(poll_seconds)
        return {"status": "Timeout", "failureReason": "Polling timed out"}

    # ── Schedule pipeline ────────────────────────────────────────────

    def schedule_pipeline(
        self,
        pipeline_item_id: str,
        interval_minutes: int = 60,
        start_datetime: str | None = None,
        end_datetime: str | None = None,
        timezone: str = "UTC",
        enabled: bool = True,
    ) -> dict:
        """
        Create a cron schedule for the pipeline.

        ``interval_minutes`` — run every N minutes (default 60 = hourly).
        Returns the schedule response dict.
        """
        url = self._items_url(f"/{pipeline_item_id}/jobs/Pipeline/schedules")
        config: dict = {
            "type": "Cron",
            "interval": interval_minutes,
            "localTimeZoneId": timezone,
        }
        if start_datetime:
            config["startDateTime"] = start_datetime
        if end_datetime:
            config["endDateTime"] = end_datetime

        body = {"enabled": enabled, "configuration": config}
        resp = requests.post(url, headers=self._headers(), json=body)
        resp.raise_for_status()
        result = resp.json()
        log.info("pipeline_scheduled", schedule_id=result.get("id"))
        return result
