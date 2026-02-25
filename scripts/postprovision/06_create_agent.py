#!/usr/bin/env python3
"""
06_create_agent.py — Create an AI Foundry agent with Foundry IQ MCPTool.

Creates:
  1. An MCP connection on the Foundry project pointing to the knowledge
     base's MCP endpoint on AI Search
  2. A persistent agent that uses MCPTool to query the knowledge base
     via agentic retrieval

Uses the v2 SDK (azure-ai-projects>=2.0.0b4) with create_version +
PromptAgentDefinition pattern.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env

import requests
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
from azure.ai.projects.models import (
    MCPTool,
    PromptAgentDefinition,
)


KNOWLEDGE_BASE_NAME = "confluence-kb"
MCP_CONNECTION_NAME = "confluence-kb-mcp"
AGENT_NAME = "confluence-data-analyst"
AGENT_MODEL = "gpt-4o"

AGENT_INSTRUCTIONS = """You are a Confluence data analyst agent. You help users
explore and understand their Confluence knowledge base data that has been
extracted, transformed, and loaded into a data lakehouse.

You have access to a knowledge base (via MCP tool) that contains the
Gold-layer aggregated data from Confluence, including:
- Page content, titles, and metadata
- Comment threads and discussions
- Space-level summaries and statistics

When answering questions:
1. Use the knowledge_base_retrieve tool to search the knowledge base first
2. Provide specific numbers and references when available
3. If the data doesn't contain the answer, say so clearly
4. Suggest follow-up queries the user might find useful
"""

VERIFICATION_QUERY = "What Confluence spaces exist? List all space names."


def create_mcp_connection(
    openai_service_id: str,
    project_name: str,
    search_name: str,
    credential: DefaultAzureCredential,
) -> str:
    """Create an MCP connection on the Foundry project via ARM REST API.

    Uses CustomKeys auth with the AI Search admin key. This is the only
    auth type that works reliably:
      - ApiKey auth is rejected by ARM for RemoteTool category connections
      - ProjectManagedIdentity is accepted but causes 403 Forbidden at runtime
        (RBAC propagation is too slow / insufficient for MCP endpoints)
      - CustomKeys auth works immediately — the search admin key is passed
        as 'api-key' in the credentials.keys object

    Returns the connection name.
    """
    project_resource_id = f"{openai_service_id}/projects/{project_name}"
    mcp_endpoint = (
        f"https://{search_name}.search.windows.net"
        f"/knowledgebases/{KNOWLEDGE_BASE_NAME}"
        f"/mcp?api-version=2025-11-01-Preview"
    )

    # Get ARM token and search admin key
    token = credential.get_token("https://management.azure.com/.default")
    search_admin_key = get_search_admin_key(credential, openai_service_id)

    url = (
        f"https://management.azure.com{project_resource_id}"
        f"/connections/{MCP_CONNECTION_NAME}"
        f"?api-version=2025-06-01"
    )

    body = {
        "name": MCP_CONNECTION_NAME,
        "properties": {
            "authType": "CustomKeys",
            "category": "RemoteTool",
            "target": mcp_endpoint,
            "isSharedToAll": True,
            "metadata": {
                "ApiType": "Azure",
            },
            "credentials": {
                "keys": {
                    "api-key": search_admin_key,
                },
            },
        },
    }

    resp = requests.put(
        url,
        headers={
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        },
        json=body,
    )

    if resp.status_code in (200, 201):
        print(
            f"    MCP connection '{MCP_CONNECTION_NAME}' created/updated (CustomKeys auth)."
        )
        return MCP_CONNECTION_NAME
    else:
        print(f"    [error] MCP connection failed: {resp.status_code}")
        print(f"    {resp.text[:500]}")
        raise RuntimeError(f"Failed to create MCP connection: {resp.status_code}")


def get_search_admin_key(
    credential: DefaultAzureCredential,
    openai_service_id: str,
) -> str:
    """Get the AI Search admin key via management API.

    Extracts subscription_id and resource_group from the openai_service_id
    which has format: /subscriptions/.../resourceGroups/.../providers/...
    """
    parts = openai_service_id.split("/")
    # /subscriptions/{sub}/resourceGroups/{rg}/providers/...
    subscription_id = parts[2]
    resource_group = parts[4]

    from azure.mgmt.search import SearchManagementClient

    env = load_azd_env()
    search_name = require_env(env, "AI_SEARCH_SERVICE_NAME")

    mgmt_client = SearchManagementClient(credential, subscription_id)
    keys = mgmt_client.admin_keys.get(resource_group, search_name)
    return keys.primary_key


def main() -> None:
    env = load_azd_env()
    project_endpoint = require_env(env, "AI_FOUNDRY_PROJECT_ENDPOINT")
    openai_service_id = require_env(env, "AZURE_OPENAI_SERVICE_ID")
    project_name = require_env(env, "AI_FOUNDRY_PROJECT_NAME")
    search_name = require_env(env, "AI_SEARCH_SERVICE_NAME")

    credential = DefaultAzureCredential()

    # ── 1. Create MCP connection on Foundry project ────────────────
    print(f"  Creating MCP connection '{MCP_CONNECTION_NAME}'...")
    mcp_connection_name = create_mcp_connection(
        openai_service_id=openai_service_id,
        project_name=project_name,
        search_name=search_name,
        credential=credential,
    )

    # ── 2. Create agent with MCPTool ───────────────────────────────
    print(f"  Creating AI Foundry agent '{AGENT_NAME}'...")
    print(f"  Project endpoint: {project_endpoint}")

    client = AIProjectClient(
        endpoint=project_endpoint,
        credential=credential,
    )

    mcp_endpoint = (
        f"https://{search_name}.search.windows.net"
        f"/knowledgebases/{KNOWLEDGE_BASE_NAME}"
        f"/mcp?api-version=2025-11-01-Preview"
    )

    mcp_kb_tool = MCPTool(
        server_label="knowledge-base",
        server_url=mcp_endpoint,
        require_approval="never",
        allowed_tools=["knowledge_base_retrieve"],
        project_connection_id=mcp_connection_name,
    )

    try:
        agent = client.agents.create_version(
            agent_name=AGENT_NAME,
            definition=PromptAgentDefinition(
                model=AGENT_MODEL,
                instructions=AGENT_INSTRUCTIONS,
                tools=[mcp_kb_tool],
            ),
            description="Confluence data analyst with Foundry IQ knowledge base grounding",
        )
        print(f"    Agent created! ID: {agent.id}")
        print(f"    Name: {agent.name}")
        print(f"    Version: {agent.version}")
    except Exception as e:
        print(f"    [error] Agent creation failed: {e}")
        print("    This may happen if RBAC roles haven't propagated yet.")
        print("    Wait 5-10 minutes and re-run this script.")
        return

    # ── 3. Verification query ──────────────────────────────────────
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
                preview = text[:500] + ("..." if len(text) > 500 else "")
                print(f"    Agent response: {preview}")
                break
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
