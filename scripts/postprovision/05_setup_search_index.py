#!/usr/bin/env python3
"""
05_setup_search_index.py — Create Foundry IQ knowledge source + knowledge base.

Replaces the manual index/datasource/indexer approach with the agentic
retrieval pipeline:
  1. Clean up old manual resources (index, datasource, indexer) if present
  2. Create an IndexedOneLakeKnowledgeSource that auto-generates the
     datasource, skillset, index, and indexer for the OneLake lakehouse
  3. Poll ingestion status until the indexer completes
  4. Create a KnowledgeBase referencing the knowledge source
  5. Print the MCP endpoint URL for the agent script

Requires azure-search-documents>=11.7.0b2.

Security note:
  The auto-generated index does not include document-level security fields.
  To add security trimming on top of the knowledge source, run:

      python scripts/postprovision/07_setup_security_filters.py

  That script creates a companion 'confluence-secure-demo' index with an
  'allowed_groups' Collection(Edm.String) filterable field and demonstrates
  query-time security filter enforcement.

  For the full security module documentation, including the upgrade path to
  native RBAC enforcement (preview), see:
      docs/security-index-access.md

  References:
    https://learn.microsoft.com/en-us/azure/search/search-document-level-access-overview
    https://learn.microsoft.com/en-us/azure/search/search-query-access-control-rbac-enforcement
"""

from __future__ import annotations

import json
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env

import requests
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    IndexedOneLakeKnowledgeSource,
    IndexedOneLakeKnowledgeSourceParameters,
    KnowledgeBase,
    KnowledgeBaseAzureOpenAIModel,
    KnowledgeRetrievalLowReasoningEffort,
    KnowledgeRetrievalOutputMode,
    KnowledgeSourceAzureOpenAIVectorizer,
    KnowledgeSourceContentExtractionMode,
    KnowledgeSourceIngestionParameters,
    KnowledgeSourceReference,
    AzureOpenAIVectorizerParameters,
)


# Old manual resource names (from previous implementation — clean up)
OLD_INDEX_NAME = "confluence-onelake-index"
OLD_DATASOURCE_NAME = "confluence-onelake-ds"
OLD_INDEXER_NAME = "confluence-onelake-indexer"

# New Foundry IQ resource names
KNOWLEDGE_SOURCE_NAME = "confluence-onelake-ks"
KNOWLEDGE_BASE_NAME = "confluence-kb"

API_VERSION = "2025-11-01-preview"
MAX_POLL_SECONDS = 600  # 10 minutes max for ingestion


def cleanup_old_resources(search_url: str, admin_key: str) -> None:
    """Delete old manually-created index, datasource, and indexer if they exist."""
    headers = {"api-key": admin_key, "Content-Type": "application/json"}

    for resource_type, name in [
        ("indexers", OLD_INDEXER_NAME),
        ("datasources", OLD_DATASOURCE_NAME),
        ("indexes", OLD_INDEX_NAME),
    ]:
        resp = requests.delete(
            f"{search_url}/{resource_type}/{name}?api-version={API_VERSION}",
            headers=headers,
        )
        if resp.status_code in (200, 204):
            print(f"    Deleted old {resource_type}/{name}")
        elif resp.status_code == 404:
            pass  # Not found — nothing to clean up
        else:
            print(f"    [warn] Cleanup {resource_type}/{name}: {resp.status_code}")


def cleanup_old_knowledge_resources(
    index_client: SearchIndexClient,
    search_url: str,
    admin_key: str,
) -> None:
    """Delete old knowledge base and knowledge source if they exist."""
    # Delete knowledge base first (it references the knowledge source)
    try:
        index_client.delete_knowledge_base(KNOWLEDGE_BASE_NAME)
        print(f"    Deleted old knowledge base '{KNOWLEDGE_BASE_NAME}'")
    except Exception:
        pass  # Not found

    # Delete knowledge source
    try:
        index_client.delete_knowledge_source(KNOWLEDGE_SOURCE_NAME)
        print(f"    Deleted old knowledge source '{KNOWLEDGE_SOURCE_NAME}'")
    except Exception:
        pass  # Not found

    # Also clean up auto-generated resources from previous knowledge source
    headers = {"api-key": admin_key, "Content-Type": "application/json"}
    for resource_type, name in [
        ("indexers", f"{KNOWLEDGE_SOURCE_NAME}-indexer"),
        ("datasources", f"{KNOWLEDGE_SOURCE_NAME}-datasource"),
        ("indexes", f"{KNOWLEDGE_SOURCE_NAME}-index"),
        ("skillsets", f"{KNOWLEDGE_SOURCE_NAME}-skillset"),
    ]:
        resp = requests.delete(
            f"{search_url}/{resource_type}/{name}?api-version={API_VERSION}",
            headers=headers,
        )
        if resp.status_code in (200, 204):
            print(f"    Deleted auto-generated {resource_type}/{name}")


def wait_for_ingestion(
    search_url: str, admin_key: str, ks_name: str, timeout: int = MAX_POLL_SECONDS
) -> bool:
    """
    Poll knowledge source ingestion status until complete or timeout.

    The synchronizationStatus field returns "active" once the sync schedule
    is running — it never transitions to "succeeded". Instead, we detect
    completion by checking lastSynchronizationState:
      - endTime is populated (sync finished)
      - itemsUpdatesFailed == 0 (no failures)
    """
    headers = {"api-key": admin_key, "Content-Type": "application/json"}
    endpoint = f"{search_url}/knowledgesources/{ks_name}/status"
    params = {"api-version": API_VERSION}

    deadline = time.time() + timeout
    poll_interval = 15  # seconds

    while time.time() < deadline:
        resp = requests.get(endpoint, params=params, headers=headers)
        if resp.status_code != 200:
            print(
                f"    [warn] Status check returned {resp.status_code}: {resp.text[:200]}"
            )
            time.sleep(poll_interval)
            continue

        data = resp.json()
        sync_status = data.get("synchronizationStatus", "unknown")
        last_sync = data.get("lastSynchronizationState") or {}
        end_time = last_sync.get("endTime")
        items_processed = last_sync.get("itemsUpdatesProcessed", 0)
        items_failed = last_sync.get("itemsUpdatesFailed", 0)

        # Also check the legacy lastResult fields as fallback
        last_result = data.get("lastResult") or {}
        indexer_status = last_result.get("status", "unknown")
        indexer_items = last_result.get("itemsProcessed", 0)

        print(
            f"    Sync: {sync_status}, "
            f"endTime: {end_time or 'pending'}, "
            f"processed: {items_processed or indexer_items}, "
            f"failed: {items_failed}"
        )

        # Primary detection: lastSynchronizationState.endTime is set
        if end_time:
            if items_failed == 0:
                print(
                    f"    Ingestion completed successfully! "
                    f"{items_processed or indexer_items} items processed."
                )
                return True
            else:
                print(f"    [error] Ingestion completed with {items_failed} failures.")
                print(f"    Full status: {json.dumps(data, indent=2)}")
                return False

        # Fallback: check if synchronizationStatus explicitly failed
        if sync_status in ("failed", "Failed"):
            print(f"    [error] Ingestion failed: {json.dumps(data, indent=2)}")
            return False

        # Fallback: check indexer-level status from lastResult
        if indexer_status in ("success", "Success"):
            print(f"    Ingestion completed (indexer status: {indexer_status}).")
            return True

        time.sleep(poll_interval)

    print(
        f"    [warn] Ingestion did not complete within {timeout}s — continuing anyway."
    )
    return False


def main() -> None:
    env = load_azd_env()
    search_name = require_env(env, "AI_SEARCH_SERVICE_NAME")
    workspace_id = require_env(env, "FABRIC_WORKSPACE_ID")
    lakehouse_id = require_env(env, "FABRIC_LAKEHOUSE_ID")
    openai_endpoint = require_env(env, "AZURE_OPENAI_ENDPOINT")
    embedding_deployment = require_env(env, "AZURE_OPENAI_EMBEDDING_DEPLOYMENT")
    model_deployment = require_env(env, "AZURE_OPENAI_MODEL_DEPLOYMENT")
    subscription_id = require_env(env, "AZURE_SUBSCRIPTION_ID")
    resource_group = require_env(env, "AZURE_RESOURCE_GROUP")

    search_url = f"https://{search_name}.search.windows.net"

    # Get admin key via management API
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.search import SearchManagementClient

    credential = DefaultAzureCredential()
    mgmt_client = SearchManagementClient(credential, subscription_id)
    keys = mgmt_client.admin_keys.get(resource_group, search_name)
    admin_key = keys.primary_key

    index_client = SearchIndexClient(
        endpoint=search_url,
        credential=AzureKeyCredential(admin_key),
    )

    # ── 1. Clean up old resources ──────────────────────────────────
    print("  Cleaning up old resources...")
    cleanup_old_resources(search_url, admin_key)
    cleanup_old_knowledge_resources(index_client, search_url, admin_key)

    # ── 2. Create OneLake knowledge source ─────────────────────────
    print(f"  Creating OneLake knowledge source '{KNOWLEDGE_SOURCE_NAME}'...")

    knowledge_source = IndexedOneLakeKnowledgeSource(
        name=KNOWLEDGE_SOURCE_NAME,
        description="Confluence ETL Gold-layer data from OneLake lakehouse",
        indexed_one_lake_parameters=IndexedOneLakeKnowledgeSourceParameters(
            fabric_workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            target_path=None,  # Index entire lakehouse
            ingestion_parameters=KnowledgeSourceIngestionParameters(
                disable_image_verbalization=True,  # CSV data, no images
                embedding_model=KnowledgeSourceAzureOpenAIVectorizer(
                    azure_open_ai_parameters=AzureOpenAIVectorizerParameters(
                        resource_url=openai_endpoint,
                        deployment_name=embedding_deployment,
                        model_name=embedding_deployment,  # model name matches deployment
                        # No api_key — search service's managed identity is used
                        # (Cognitive Services OpenAI User role assigned in 04_setup_rbac.py)
                    )
                ),
                # chat_completion_model omitted — not allowed when
                # disable_image_verbalization is True (MINIMAL mode)
                content_extraction_mode=KnowledgeSourceContentExtractionMode.MINIMAL,
            ),
        ),
    )

    index_client.create_or_update_knowledge_source(knowledge_source)
    print(f"    Knowledge source '{KNOWLEDGE_SOURCE_NAME}' created.")

    # ── 3. Wait for ingestion to complete ──────────────────────────
    print("  Waiting for knowledge source ingestion...")
    wait_for_ingestion(search_url, admin_key, KNOWLEDGE_SOURCE_NAME)

    # ── 4. Create knowledge base ───────────────────────────────────
    print(f"  Creating knowledge base '{KNOWLEDGE_BASE_NAME}'...")

    # Provide the LLM model for answer synthesis
    aoai_params = AzureOpenAIVectorizerParameters(
        resource_url=openai_endpoint,
        deployment_name=model_deployment,
        model_name=model_deployment,
        # No api_key — search service's managed identity is used
    )

    knowledge_base = KnowledgeBase(
        name=KNOWLEDGE_BASE_NAME,
        description="Knowledge base for Confluence ETL data grounded on OneLake",
        knowledge_sources=[
            KnowledgeSourceReference(name=KNOWLEDGE_SOURCE_NAME),
        ],
        models=[KnowledgeBaseAzureOpenAIModel(azure_open_ai_parameters=aoai_params)],
        output_mode=KnowledgeRetrievalOutputMode.ANSWER_SYNTHESIS,
        answer_instructions=(
            "Provide a concise and informative answer based on the retrieved "
            "Confluence data. Include specific details like page titles, space "
            "names, and statistics when available."
        ),
        retrieval_reasoning_effort=KnowledgeRetrievalLowReasoningEffort(),
    )

    index_client.create_or_update_knowledge_base(knowledge_base)
    print(f"    Knowledge base '{KNOWLEDGE_BASE_NAME}' created.")

    # ── 5. Print MCP endpoint ──────────────────────────────────────
    mcp_endpoint = (
        f"{search_url}/knowledgebases/{KNOWLEDGE_BASE_NAME}"
        f"/mcp?api-version={API_VERSION}"
    )
    print()
    print(f"  MCP endpoint: {mcp_endpoint}")
    print(f"  Done! Knowledge source and knowledge base are ready.")


if __name__ == "__main__":
    main()
