#!/usr/bin/env python3
"""
04_setup_rbac.py — Grant RBAC role assignments for cross-service access.

Role assignments:
  1. AI Search managed identity -> Contributor on Fabric workspace
     (so AI Search indexer can read OneLake data)
  2. AI Search managed identity -> Cognitive Services OpenAI User on OpenAI resource
     (so AI Search can use OpenAI for vectorization)
  3. Current user -> Cognitive Services OpenAI User on OpenAI resource
     (so the user can test the agent locally)
"""

from __future__ import annotations

import sys
import os
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env

from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters


# Well-known role definition IDs
ROLE_CONTRIBUTOR = "b24988ac-6180-42a0-ab88-20f7382dd24c"
ROLE_COGNITIVE_SERVICES_OPENAI_USER = "5e0bd9bd-7b93-4f28-af87-19fc36ad61bd"
ROLE_STORAGE_BLOB_DATA_CONTRIBUTOR = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"


def assign_role(
    auth_client: AuthorizationManagementClient,
    scope: str,
    role_definition_id: str,
    principal_id: str,
    principal_type: str = "ServicePrincipal",
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
        print(f"    Assigned role {role_definition_id} to {principal_id}")
    except Exception as e:
        if "Conflict" in str(e) or "RoleAssignmentExists" in str(e):
            print(f"    Role already assigned (idempotent).")
        else:
            print(f"    [warn] Role assignment failed: {e}")


def main() -> None:
    env = load_azd_env()
    subscription_id = require_env(env, "AZURE_SUBSCRIPTION_ID")
    resource_group = require_env(env, "AZURE_RESOURCE_GROUP")
    search_principal_id = require_env(env, "AI_SEARCH_PRINCIPAL_ID")
    openai_service_id = require_env(env, "AZURE_OPENAI_SERVICE_ID")

    credential = DefaultAzureCredential()
    auth_client = AuthorizationManagementClient(credential, subscription_id)

    # ── 1. AI Search -> Cognitive Services OpenAI User on OpenAI ──
    print(
        "  Granting AI Search -> Cognitive Services OpenAI User on OpenAI resource..."
    )
    assign_role(
        auth_client,
        scope=openai_service_id,
        role_definition_id=ROLE_COGNITIVE_SERVICES_OPENAI_USER,
        principal_id=search_principal_id,
        principal_type="ServicePrincipal",
    )

    # ── 2. AI Search -> Contributor on Fabric workspace ────────────
    # Note: For OneLake access, AI Search needs Contributor on the
    # Fabric workspace. This is done via the Fabric REST API workspace
    # role assignments, not ARM RBAC. We'll handle it here as a
    # best-effort via Fabric API.
    workspace_id = env.get("FABRIC_WORKSPACE_ID", "")
    if workspace_id and search_principal_id:
        print("  Granting AI Search -> Contributor on Fabric workspace...")
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

    print("  Done! RBAC role assignments configured.")


if __name__ == "__main__":
    main()
