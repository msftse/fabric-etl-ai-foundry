#!/usr/bin/env python3
"""
06_create_agent.py — Create an AI Foundry agent with Azure AI Search tool.

Creates a persistent agent in the AI Foundry project that uses
AzureAISearchAgentTool to query the OneLake-indexed data in AI Search.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env

from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import (
    AzureAISearchTool,
    ConnectionProperties,
    ConnectionType,
)


INDEX_NAME = "confluence-onelake-index"
AGENT_NAME = "Confluence Data Analyst"
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
    search_service_name = require_env(env, "AI_SEARCH_SERVICE_NAME")

    print(f"  Creating AI Foundry agent '{AGENT_NAME}'...")
    print(f"  Project endpoint: {project_endpoint}")

    credential = DefaultAzureCredential()
    client = AIProjectClient(
        endpoint=project_endpoint,
        credential=credential,
    )

    # Get the AI Search connection from the project
    # The connection was created by Bicep as part of the Hub
    search_connection = None
    try:
        connections = client.connections.list()
        for conn in connections:
            if conn.connection_type in (
                ConnectionType.COGNITIVE_SEARCH,
                "CognitiveSearch",
            ):
                search_connection = conn
                break
    except Exception as e:
        print(f"    [warn] Could not list connections: {e}")

    # Create the AI Search tool
    tools = []
    tool_resources = {}

    if search_connection:
        print(f"    Using AI Search connection: {search_connection.id}")
        ai_search_tool = AzureAISearchTool()
        ai_search_tool.add_index(
            connection_id=search_connection.id,
            index_name=INDEX_NAME,
        )
        tools.extend(ai_search_tool.definitions)
        tool_resources.update(ai_search_tool.resources)
    else:
        print("    [warn] No AI Search connection found in project.")
        print("    Creating agent without search tool (you can add it manually).")

    # Create the agent
    try:
        agent = client.agents.create_agent(
            model=AGENT_MODEL,
            name=AGENT_NAME,
            instructions=AGENT_INSTRUCTIONS,
            tools=tools if tools else None,
            tool_resources=tool_resources if tool_resources else None,
        )
        print(f"    Agent created! ID: {agent.id}")
        print(f"    Name: {agent.name}")
        print(f"    Model: {agent.model}")
    except Exception as e:
        print(f"    [error] Agent creation failed: {e}")
        print("    You can create the agent manually in AI Foundry Studio.")
        return

    print("  Done! AI Foundry agent is ready.")
    print(f"  Test it at: https://ai.azure.com")


if __name__ == "__main__":
    main()
