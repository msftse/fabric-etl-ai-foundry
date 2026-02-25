#!/usr/bin/env python3
"""
06_create_agent.py — Create an AI Foundry agent with Azure AI Search tool.

Creates a persistent agent in the Foundry-native project that uses
AzureAISearchTool to query the OneLake-indexed Confluence data via
the AI Search connection provisioned by Bicep.

Uses the Foundry-native project endpoint format:
  https://<aiServicesName>.services.ai.azure.com/api/projects/<projectName>
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env

from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.agents.models import AzureAISearchTool, AzureAISearchQueryType


INDEX_NAME = "confluence-onelake-index"
AGENT_NAME = "confluence-data-analyst"
AGENT_MODEL = "gpt-4o"

AGENT_INSTRUCTIONS = """You are a Confluence data analyst agent. You help users
explore and understand their Confluence knowledge base data that has been
extracted, transformed, and loaded into a data lakehouse.

You have access to an Azure AI Search index that contains the Gold-layer
aggregated data from Confluence, including:
- Page content, titles, and metadata
- Comment threads and discussions
- Space-level summaries and statistics

When answering questions:
1. Search the knowledge base first to ground your answers in actual data
2. Provide specific numbers and references when available
3. If the data doesn't contain the answer, say so clearly
4. Suggest follow-up queries the user might find useful
"""


def main() -> None:
    env = load_azd_env()
    project_endpoint = require_env(env, "AI_FOUNDRY_PROJECT_ENDPOINT")
    search_connection_name = require_env(env, "AI_SEARCH_CONNECTION_NAME")

    print(f"  Creating AI Foundry agent '{AGENT_NAME}'...")
    print(f"  Project endpoint: {project_endpoint}")
    print(f"  Search connection: {search_connection_name}")

    credential = DefaultAzureCredential()
    client = AIProjectClient(
        endpoint=project_endpoint,
        credential=credential,
    )

    # ── Get the AI Search connection from the project ──────────────
    print(f"  Looking up connection '{search_connection_name}'...")
    try:
        conn = client.connections.get(search_connection_name)
        print(f"    Found connection: {conn.name} (id={conn.id})")
        print(f"    Target: {conn.target}")
    except Exception as e:
        print(f"    [error] Could not get connection '{search_connection_name}': {e}")
        print("    Ensure the Bicep deployment created the connection.")
        return

    # ── Create the AI Search tool ──────────────────────────────────
    ai_search = AzureAISearchTool(
        index_connection_id=conn.id,
        index_name=INDEX_NAME,
        query_type=AzureAISearchQueryType.SIMPLE,
        top_k=5,
    )

    # ── Create the agent ───────────────────────────────────────────
    try:
        agent = client.agents.create_agent(
            model=AGENT_MODEL,
            name=AGENT_NAME,
            instructions=AGENT_INSTRUCTIONS,
            tools=ai_search.definitions,
            tool_resources=ai_search.resources,
        )
        print(f"    Agent created! ID: {agent.id}")
        print(f"    Name: {agent.name}")
        print(f"    Model: {agent.model}")
    except Exception as e:
        print(f"    [error] Agent creation failed: {e}")
        print("    This may happen if RBAC roles haven't propagated yet.")
        print("    Wait 5-10 minutes and re-run this script.")
        return

    print("  Done! AI Foundry agent is ready.")
    print(f"  Test it at: https://ai.azure.com")


if __name__ == "__main__":
    main()
