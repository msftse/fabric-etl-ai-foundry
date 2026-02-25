#!/usr/bin/env python3
"""
04_setup_rbac.py — Grant RBAC role assignments and create capability hosts.

Role assignments (8 total):
  1. AI Search managed identity -> Cognitive Services OpenAI User on OpenAI resource
     (so AI Search can use OpenAI for vectorization)
  2. AI Search managed identity -> Contributor on Fabric workspace
     (so AI Search indexer can read OneLake data)
  3. AI Services account identity -> Search Index Data Reader on AI Search
     (so the agent runtime can read index data)
  4. AI Services account identity -> Search Service Contributor on AI Search
     (so the agent runtime can manage search resources)
  5. Foundry project identity -> Search Index Data Contributor on AI Search
     (so the project can write to the search index)
  6. Foundry project identity -> Search Service Contributor on AI Search
     (so the project can manage search resources)
  7. Foundry project identity -> Search Index Data Reader on AI Search
     (so the agent runtime under the project identity can query the index)
  8. Foundry project identity -> Cognitive Services OpenAI User on AI Services
     (so the project identity can call OpenAI models for agent + knowledge base)

Capability hosts:
  - Account-level capability host (enables Agent Service on the AI Services account)
  - Project-level capability host (enables Agent Service on the Foundry project)
"""

from __future__ import annotations

import sys
import os
import uuid
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env

import requests
from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters


# Well-known role definition IDs
ROLE_CONTRIBUTOR = "b24988ac-6180-42a0-ab88-20f7382dd24c"
ROLE_COGNITIVE_SERVICES_OPENAI_USER = "5e0bd9bd-7b93-4f28-af87-19fc36ad61bd"
ROLE_SEARCH_INDEX_DATA_READER = "1407120a-92aa-4202-b7e9-c0e197c71c8f"
ROLE_SEARCH_INDEX_DATA_CONTRIBUTOR = "8ebe5a00-799e-43f5-93ac-243d3dce84a7"
ROLE_SEARCH_SERVICE_CONTRIBUTOR = "7ca78c08-252a-4471-8644-bb5ff32d4ba0"


def assign_role(
    auth_client: AuthorizationManagementClient,
    scope: str,
    role_definition_id: str,
    principal_id: str,
    principal_type: str = "ServicePrincipal",
    label: str = "",
) -> None:
    """Create a role assignment (idempotent — ignores 409 Conflict)."""
    full_role_id = f"{scope}/providers/Microsoft.Authorization/roleDefinitions/{role_definition_id}"
    assignment_name = str(uuid.uuid4())

    try:
        auth_client.role_assignments.create(
            scope=scope,
            role_assignment_name=assignment_name,
            parameters=RoleAssignmentCreateParameters(
                role_definition_id=full_role_id,
                principal_id=principal_id,
                principal_type=principal_type,
            ),
        )
        print(f"    Assigned {label or role_definition_id} to {principal_id}")
    except Exception as e:
        if "Conflict" in str(e) or "RoleAssignmentExists" in str(e):
            print(
                f"    Role already assigned ({label or role_definition_id}) — idempotent."
            )
        else:
            print(f"    [warn] Role assignment failed ({label}): {e}")


def create_capability_hosts(
    openai_service_id: str,
    project_name: str,
    credential: DefaultAzureCredential,
) -> None:
    """Create account-level and project-level capability hosts for Agent Service.

    These are required to enable the Agent Service data-plane. Without them,
    the project endpoint cannot serve agent requests. They provision almost
    instantly (Succeeded on first poll).
    """
    token = credential.get_token("https://management.azure.com/.default")
    headers = {
        "Authorization": f"Bearer {token.token}",
        "Content-Type": "application/json",
    }
    body = {"properties": {"capabilityHostKind": "Agents"}}

    # Account-level capability host
    account_url = (
        f"https://management.azure.com{openai_service_id}"
        f"/capabilityHosts/default?api-version=2025-06-01"
    )
    print("    Creating account-level capability host...")
    resp = requests.put(account_url, headers=headers, json=body)
    if resp.status_code in (200, 201):
        print("    Account capability host created/exists.")
    else:
        print(
            f"    [warn] Account capability host: {resp.status_code} {resp.text[:300]}"
        )

    # Wait briefly for account-level to settle before creating project-level
    time.sleep(5)

    # Project-level capability host
    project_url = (
        f"https://management.azure.com{openai_service_id}"
        f"/projects/{project_name}/capabilityHosts/default"
        f"?api-version=2025-06-01"
    )
    print("    Creating project-level capability host...")
    resp = requests.put(project_url, headers=headers, json=body)
    if resp.status_code in (200, 201):
        print("    Project capability host created/exists.")
    else:
        print(
            f"    [warn] Project capability host: {resp.status_code} {resp.text[:300]}"
        )

    # Poll for provisioning completion (typically instant)
    for _ in range(6):
        time.sleep(5)
        resp = requests.get(project_url, headers=headers)
        if resp.status_code == 200:
            state = resp.json().get("properties", {}).get("provisioningState", "")
            if state == "Succeeded":
                print("    Capability hosts provisioned successfully.")
                return
            print(f"    Provisioning state: {state}, waiting...")
    print(
        "    [warn] Capability host provisioning did not complete in time; continuing."
    )


def main() -> None:
    env = load_azd_env()
    subscription_id = require_env(env, "AZURE_SUBSCRIPTION_ID")
    resource_group = require_env(env, "AZURE_RESOURCE_GROUP")
    search_principal_id = require_env(env, "AI_SEARCH_PRINCIPAL_ID")
    search_service_id = require_env(env, "AI_SEARCH_SERVICE_ID")
    openai_service_id = require_env(env, "AZURE_OPENAI_SERVICE_ID")
    openai_principal_id = require_env(env, "AZURE_OPENAI_PRINCIPAL_ID")
    project_principal_id = require_env(env, "AI_FOUNDRY_PROJECT_PRINCIPAL_ID")

    credential = DefaultAzureCredential()
    auth_client = AuthorizationManagementClient(credential, subscription_id)

    # ── 1. AI Search -> Cognitive Services OpenAI User on OpenAI ──
    print("  1. AI Search -> Cognitive Services OpenAI User on OpenAI...")
    assign_role(
        auth_client,
        scope=openai_service_id,
        role_definition_id=ROLE_COGNITIVE_SERVICES_OPENAI_USER,
        principal_id=search_principal_id,
        label="CognitiveServicesOpenAIUser",
    )

    # ── 2. AI Search -> Contributor on Fabric workspace ────────────
    workspace_id = env.get("FABRIC_WORKSPACE_ID", "")
    if workspace_id and search_principal_id:
        print("  2. AI Search -> Contributor on Fabric workspace...")
        try:
            from _helpers import FabricClient

            fabric = FabricClient()
            resp = fabric.post(
                f"/workspaces/{workspace_id}/roleAssignments",
                {
                    "principal": {
                        "id": search_principal_id,
                        "type": "ServicePrincipal",
                    },
                    "role": "Contributor",
                },
            )
            if resp.status_code in (200, 201):
                print("    Fabric workspace role assigned.")
            elif resp.status_code == 409:
                print("    Fabric workspace role already assigned.")
            else:
                print(
                    f"    [warn] Fabric role assignment: {resp.status_code} {resp.text}"
                )
        except Exception as e:
            print(f"    [warn] Fabric workspace role assignment failed: {e}")
    else:
        print("  2. [skip] FABRIC_WORKSPACE_ID not set yet — will be set by script 01.")

    # ── 3. AI Services account -> Search Index Data Reader on AI Search ──
    print("  3. AI Services -> Search Index Data Reader on AI Search...")
    assign_role(
        auth_client,
        scope=search_service_id,
        role_definition_id=ROLE_SEARCH_INDEX_DATA_READER,
        principal_id=openai_principal_id,
        label="SearchIndexDataReader",
    )

    # ── 4. AI Services account -> Search Service Contributor on AI Search ──
    print("  4. AI Services -> Search Service Contributor on AI Search...")
    assign_role(
        auth_client,
        scope=search_service_id,
        role_definition_id=ROLE_SEARCH_SERVICE_CONTRIBUTOR,
        principal_id=openai_principal_id,
        label="SearchServiceContributor",
    )

    # ── 5. Foundry project -> Search Index Data Contributor on AI Search ──
    print("  5. Foundry project -> Search Index Data Contributor on AI Search...")
    assign_role(
        auth_client,
        scope=search_service_id,
        role_definition_id=ROLE_SEARCH_INDEX_DATA_CONTRIBUTOR,
        principal_id=project_principal_id,
        label="SearchIndexDataContributor",
    )

    # ── 6. Foundry project -> Search Service Contributor on AI Search ──
    print("  6. Foundry project -> Search Service Contributor on AI Search...")
    assign_role(
        auth_client,
        scope=search_service_id,
        role_definition_id=ROLE_SEARCH_SERVICE_CONTRIBUTOR,
        principal_id=project_principal_id,
        label="SearchServiceContributor",
    )

    # ── 7. Foundry project -> Search Index Data Reader on AI Search ──
    print("  7. Foundry project -> Search Index Data Reader on AI Search...")
    assign_role(
        auth_client,
        scope=search_service_id,
        role_definition_id=ROLE_SEARCH_INDEX_DATA_READER,
        principal_id=project_principal_id,
        label="SearchIndexDataReader",
    )

    # ── 8. Foundry project -> Cognitive Services OpenAI User on AI Services ──
    print("  8. Foundry project -> Cognitive Services OpenAI User on AI Services...")
    assign_role(
        auth_client,
        scope=openai_service_id,
        role_definition_id=ROLE_COGNITIVE_SERVICES_OPENAI_USER,
        principal_id=project_principal_id,
        label="CognitiveServicesOpenAIUser",
    )

    print("  Done! RBAC role assignments configured (8 assignments).")
    print("  Note: RBAC propagation may take up to 10 minutes.")

    # ── Capability hosts ───────────────────────────────────────────
    # Required to enable the Agent Service data-plane on the AI Services
    # account and Foundry project. Without these, the project endpoint
    # cannot serve agent requests.
    print()
    print("  Creating capability hosts for Agent Service...")
    create_capability_hosts(
        openai_service_id=openai_service_id,
        project_name=require_env(env, "AI_FOUNDRY_PROJECT_NAME"),
        credential=credential,
    )


if __name__ == "__main__":
    main()
