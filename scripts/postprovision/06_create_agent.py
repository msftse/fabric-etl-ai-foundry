#!/usr/bin/env python3
"""
06_create_agent.py — Create an AI Foundry agent with Azure AI Search tool.

Creates a persistent agent in the Foundry-native project that uses
AzureAISearchTool to query the OneLake-indexed Confluence data via
the AI Search connection provisioned by Bicep.

Uses the v2 SDK (azure-ai-projects>=2.0.0b4) with create_version +
PromptAgentDefinition pattern, which routes through the agents backend
(/api/projects/<name>/agents/<name>/versions) instead of the broken
/api/projects/<name>/assistants endpoint.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env

from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import (
    PromptAgentDefinition,
    AzureAISearchTool,
    AzureAISearchToolResource,
    AISearchIndexResource,
    AzureAISearchQueryType,
)


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

VERIFICATION_QUERY = "What Confluence spaces exist? List all space names."


def main() -> None:
    env = load_azd_env()
    project_endpoint = require_env(env, "AI_FOUNDRY_PROJECT_ENDPOINT")
    search_connection_name = require_env(env, "AI_SEARCH_CONNECTION_NAME")
    openai_service_id = require_env(env, "AZURE_OPENAI_SERVICE_ID")
    project_name = require_env(env, "AI_FOUNDRY_PROJECT_NAME")

    # Build the ARM resource ID for the search connection on the project.
    # Format: <aiServicesResourceId>/projects/<projectName>/connections/<connectionName>
    connection_id = (
        f"{openai_service_id}/projects/{project_name}"
        f"/connections/{search_connection_name}"
    )

    print(f"  Creating AI Foundry agent '{AGENT_NAME}'...")
    print(f"  Project endpoint: {project_endpoint}")
    print(f"  Search connection ID: {connection_id}")

    credential = DefaultAzureCredential()
    client = AIProjectClient(
        endpoint=project_endpoint,
        credential=credential,
    )

    # ── Build the AI Search tool definition ────────────────────────
    search_tool = AzureAISearchTool(
        azure_ai_search=AzureAISearchToolResource(
            indexes=[
                AISearchIndexResource(
                    project_connection_id=connection_id,
                    index_name=INDEX_NAME,
                    query_type=AzureAISearchQueryType.SIMPLE,
                    top_k=5,
                )
            ]
        )
    )

    # ── Create the agent using v2 create_version API ───────────────
    try:
        agent = client.agents.create_version(
            agent_name=AGENT_NAME,
            definition=PromptAgentDefinition(
                model=AGENT_MODEL,
                instructions=AGENT_INSTRUCTIONS,
                tools=[search_tool],
            ),
            description="Confluence data analyst with AI Search grounding on OneLake data",
        )
        print(f"    Agent created! ID: {agent.id}")
        print(f"    Name: {agent.name}")
        print(f"    Version: {agent.version}")
    except Exception as e:
        print(f"    [error] Agent creation failed: {e}")
        print("    This may happen if RBAC roles haven't propagated yet.")
        print("    Wait 5-10 minutes and re-run this script.")
        return

    # ── Verification query ─────────────────────────────────────────
    print()
    print(f"  Verifying agent with query: '{VERIFICATION_QUERY}'")
    try:
        openai_client = client.get_openai_client()
        response = openai_client.responses.create(
            model=AGENT_MODEL,
            input=VERIFICATION_QUERY,
            tool_choice="required",
            extra_body={
                "agent_reference": {
                    "name": agent.name,
                    "type": "agent_reference",
                }
            },
            stream=False,
        )
        # Extract text from response
        for item in response.output:
            text = getattr(item, "text", None)
            if text:
                # Truncate for readability
                preview = text[:500] + ("..." if len(text) > 500 else "")
                print(f"    Agent response: {preview}")
                break
            # Also check content array (message items)
            content = getattr(item, "content", None)
            if content:
                for part in content:
                    t = getattr(part, "text", None)
                    if t:
                        preview = t[:500] + ("..." if len(t) > 500 else "")
                        print(f"    Agent response: {preview}")
                        break
                break
        print("    Verification passed!")
    except Exception as e:
        err_msg = str(e)
        if "Access denied" in err_msg:
            print("    [warn] Agent query returned 'Access denied'.")
            print("    RBAC roles may need up to 10 minutes to propagate.")
            print("    The agent was created successfully; re-test after waiting.")
        else:
            print(f"    [warn] Verification query failed: {e}")
            print(
                "    The agent was created; query issues may resolve after RBAC propagation."
            )

    print()
    print("  Done! AI Foundry agent is ready.")
    print(f"  Test it at: https://ai.azure.com")


if __name__ == "__main__":
    main()
