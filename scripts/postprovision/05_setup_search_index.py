#!/usr/bin/env python3
"""
05_setup_search_index.py — Create AI Search index + OneLake data source + indexer.

Creates an Azure AI Search index that indexes CSV files from the Gold layer
in OneLake, enabling the AI Foundry agent to query Confluence data via
a knowledge base grounded on the indexed content.
"""

from __future__ import annotations

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env

from azure.identity import DefaultAzureCredential
from azure.mgmt.search import SearchManagementClient

import requests


INDEX_NAME = "confluence-onelake-index"
DATASOURCE_NAME = "confluence-onelake-ds"
INDEXER_NAME = "confluence-onelake-indexer"


def get_search_admin_key(
    subscription_id: str, resource_group: str, search_name: str
) -> str:
    """Get the admin key for the search service."""
    credential = DefaultAzureCredential()
    mgmt_client = SearchManagementClient(credential, subscription_id)
    keys = mgmt_client.admin_keys.get(resource_group, search_name)
    return keys.primary_key


def main() -> None:
    env = load_azd_env()
    subscription_id = require_env(env, "AZURE_SUBSCRIPTION_ID")
    resource_group = require_env(env, "AZURE_RESOURCE_GROUP")
    search_name = require_env(env, "AI_SEARCH_SERVICE_NAME")
    workspace_name = env.get("FABRIC_WORKSPACE_NAME", "confluence-etl")
    lakehouse_name = env.get("FABRIC_LAKEHOUSE_NAME", "confluencelakehouse")

    search_url = f"https://{search_name}.search.windows.net"
    admin_key = get_search_admin_key(subscription_id, resource_group, search_name)

    headers = {
        "Content-Type": "application/json",
        "api-key": admin_key,
    }
    api_version = "2024-07-01"

    # ── 1. Create index ────────────────────────────────────────────
    print(f"  Creating search index '{INDEX_NAME}'...")
    index_def = {
        "name": INDEX_NAME,
        "fields": [
            {"name": "id", "type": "Edm.String", "key": True, "filterable": True},
            {
                "name": "content",
                "type": "Edm.String",
                "searchable": True,
                "retrievable": True,
            },
            {
                "name": "metadata_storage_path",
                "type": "Edm.String",
                "filterable": True,
                "retrievable": True,
            },
            {
                "name": "metadata_storage_name",
                "type": "Edm.String",
                "filterable": True,
                "retrievable": True,
            },
            {
                "name": "metadata_storage_last_modified",
                "type": "Edm.DateTimeOffset",
                "filterable": True,
                "sortable": True,
            },
        ],
    }

    resp = requests.put(
        f"{search_url}/indexes/{INDEX_NAME}?api-version={api_version}",
        headers=headers,
        json=index_def,
    )
    if resp.status_code in (200, 201):
        print(f"    Index created/updated.")
    else:
        print(f"    Index response: {resp.status_code} {resp.text}")

    # ── 2. Create OneLake data source ──────────────────────────────
    print(f"  Creating OneLake data source '{DATASOURCE_NAME}'...")

    # OneLake connection string for AI Search
    # Format: ResourceId=/subscriptions/.../providers/Microsoft.Storage/storageAccounts/onelake;
    onelake_connection = (
        f"ResourceId=/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Storage/storageAccounts/onelake"
    )
    container = f"{workspace_name}/{lakehouse_name}.Lakehouse/Files/gold"

    datasource_def = {
        "name": DATASOURCE_NAME,
        "type": "adlsgen2",
        "credentials": {"connectionString": onelake_connection},
        "container": {"name": container},
    }

    resp = requests.put(
        f"{search_url}/datasources/{DATASOURCE_NAME}?api-version={api_version}",
        headers=headers,
        json=datasource_def,
    )
    if resp.status_code in (200, 201):
        print(f"    Data source created/updated.")
    else:
        print(f"    Data source response: {resp.status_code} {resp.text}")

    # ── 3. Create indexer ──────────────────────────────────────────
    print(f"  Creating indexer '{INDEXER_NAME}'...")
    indexer_def = {
        "name": INDEXER_NAME,
        "dataSourceName": DATASOURCE_NAME,
        "targetIndexName": INDEX_NAME,
        "parameters": {
            "configuration": {
                "parsingMode": "delimitedText",
                "firstLineContainsHeaders": True,
            }
        },
        "schedule": {"interval": "PT1H"},
    }

    resp = requests.put(
        f"{search_url}/indexers/{INDEXER_NAME}?api-version={api_version}",
        headers=headers,
        json=indexer_def,
    )
    if resp.status_code in (200, 201):
        print(f"    Indexer created/updated.")
    else:
        print(f"    Indexer response: {resp.status_code} {resp.text}")

    # ── 4. Run indexer ─────────────────────────────────────────────
    print("  Running indexer...")
    resp = requests.post(
        f"{search_url}/indexers/{INDEXER_NAME}/run?api-version={api_version}",
        headers=headers,
    )
    if resp.status_code in (200, 202):
        print("    Indexer run triggered.")
    else:
        print(f"    Indexer run response: {resp.status_code} {resp.text}")

    # Wait briefly for indexer to start processing
    print("  Waiting 30s for indexer to process...")
    time.sleep(30)

    # Check status
    resp = requests.get(
        f"{search_url}/indexers/{INDEXER_NAME}/status?api-version={api_version}",
        headers=headers,
    )
    if resp.status_code == 200:
        data = resp.json()
        last_result = data.get("lastResult", {})
        status = last_result.get("status", "unknown")
        doc_count = last_result.get("itemsProcessed", 0)
        print(f"    Indexer status: {status}, documents processed: {doc_count}")

    print(f"  Done! Search index '{INDEX_NAME}' is ready.")


if __name__ == "__main__":
    main()
