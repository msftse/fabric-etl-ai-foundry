# Module: Securing Data Access in Azure AI Search

This module extends the Fabric ETL + AI Foundry project with **document-level access control** for the AI Search index, ensuring that users only retrieve documents they are authorized to see.

**References:**
- [Document-level access control — Azure AI Search](https://learn.microsoft.com/en-us/azure/search/search-document-level-access-overview)
- [Query-time ACL and RBAC enforcement — Azure AI Search](https://learn.microsoft.com/en-us/azure/search/search-query-access-control-rbac-enforcement)

---

## Why This Matters

In this project the AI Foundry agent queries an AI Search index that is grounded on **OneLake Gold-layer data** — Confluence pages, comments, and aggregations that may be sensitive to specific teams or individuals. Without document-level access control:

- Every user who can reach the agent gets access to every indexed document.
- There is no way to restrict results to a user's own team or space.
- Compliance requirements (e.g., data residency, need-to-know) cannot be enforced at query time.

Azure AI Search offers three complementary approaches. This module implements the **Security Filters (string comparison)** pattern — the most broadly compatible approach — and explains how to upgrade to native RBAC-scope enforcement (preview) once it is generally available.

---

## Approach Overview

| Approach | When to Use | Status |
|---|---|---|
| **Security filters (string comparison)** | Any identity source, custom ACL model, full API compatibility | GA — used in this module |
| **POSIX-like ACL / RBAC scopes** | ADLS Gen2 or Azure Blob with Entra-native ACLs | Preview — see [Upgrading to Native RBAC Scopes](#upgrading-to-native-rbac-scopes) |
| **Microsoft Purview sensitivity labels** | Enterprise label-based access aligned with Purview policies | Preview |
| **SharePoint in M365 ACLs** | SharePoint-indexed content | Preview |

---

## Architecture: Security Filters

The security-filter approach works by storing group/user identifiers as a filterable string field on every indexed document, then injecting a filter at query time that restricts results to documents tagged with the caller's identity.

```
  ┌─────────────────────────────────────────────────────────┐
  │                  GOLD LAYER (OneLake)                   │
  │                                                         │
  │  confluence_pages.csv          confluence_spaces.csv    │
  │  ┌───────────────────┐         ┌──────────────────────┐ │
  │  │ page_id           │         │ space_key            │ │
  │  │ title             │         │ space_name           │ │
  │  │ body_text         │         │ ...                  │ │
  │  │ allowed_groups ◄──┼─────────┼── NEW FIELD          │ │
  │  └───────────────────┘         └──────────────────────┘ │
  └─────────────────────────────────────────────────────────┘
                        │ CSV indexing
                        ▼
  ┌─────────────────────────────────────────────────────────┐
  │              AZURE AI SEARCH INDEX                      │
  │                                                         │
  │  Field: allowed_groups  (filterable: true)              │
  │  Contains: ["team-data", "team-platform", "all"]        │
  └─────────────────────────────────────────────────────────┘
                        │ query with filter
                        ▼
  ┌─────────────────────────────────────────────────────────┐
  │              AI FOUNDRY AGENT                           │
  │                                                         │
  │  Query:  "search=*&$filter=allowed_groups/any(g: g eq  │
  │           'team-data')"                                  │
  │                                                         │
  │  Only returns documents tagged for team-data            │
  └─────────────────────────────────────────────────────────┘
```

---

## Step 1: Add `allowed_groups` to the Gold-Layer Data

The security field is populated **during the ETL pipeline**, before data reaches the index.

In this project, each Confluence space maps to a logical team. The ETL Gold layer (`src/etl/gold/confluence_aggregation.py`) produces the `confluence_content_by_space` table. You can add an `allowed_groups` column that maps space keys to Entra group names or custom identifiers.

**Example mapping (extend to your organization):**

```python
# src/etl/gold/confluence_aggregation.py (Gold layer extension)

# Map Confluence space keys to allowed security groups.
# In production, retrieve this mapping from your identity provider or
# a configuration store rather than hard-coding it.
SPACE_GROUP_MAP: dict[str, list[str]] = {
    "ETLDEMO": ["team-data-engineering", "all"],
    "PLAT":    ["team-platform", "all"],
    "FIN":     ["team-finance"],
    "HR":      ["team-hr"],
    # Fallback: spaces without an explicit mapping are visible to "all"
}


def add_security_field(df: pd.DataFrame, key_col: str = "space_key") -> pd.DataFrame:
    """
    Append an 'allowed_groups' column (comma-separated string) that AI Search
    can store as a filterable Collection(Edm.String) field.

    Using a single comma-separated string keeps the CSV format simple.
    The AI Search index maps this to Collection(Edm.String) with 'filterable: true'.
    """
    def get_groups(space_key: str) -> str:
        groups = SPACE_GROUP_MAP.get(str(space_key).upper(), ["all"])
        return ",".join(groups)

    df = df.copy()
    df["allowed_groups"] = df[key_col].apply(get_groups)
    return df
```

Call `add_security_field(df)` before writing CSV files in the Gold layer aggregation step.

---

## Step 2: Update the AI Search Index Schema

The index must declare `allowed_groups` as a **filterable** `Collection(Edm.String)` field. This is handled by `scripts/postprovision/07_setup_security_filters.py` (introduced in this module).

Key field definition:

```python
SimpleField(
    name="allowed_groups",
    type=SearchFieldDataType.Collection(SearchFieldDataType.String),
    filterable=True,
    retrievable=True,
)
```

> **Note:** The field must be `filterable: true` for the security filter to work. It does **not** need to be searchable.

---

## Step 3: Enforce the Filter at Query Time

When the AI Foundry agent (or any client) queries the index, it must pass the caller's group membership as an OData `$filter` expression:

```http
POST https://<search-service>.search.windows.net/indexes/<index>/docs/search?api-version=2024-07-01
Authorization: Bearer <SERVICE_API_KEY>
Content-Type: application/json

{
  "search": "What are the most discussed pages?",
  "filter": "allowed_groups/any(g: g eq 'team-data-engineering') or allowed_groups/any(g: g eq 'all')",
  "select": "id, title, space_key, allowed_groups"
}
```

### In the AI Foundry Agent (Python)

The agent in `src/ai_agent/analyst.py` can be extended to inject the security filter:

```python
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential


def build_security_filter(user_groups: list[str]) -> str:
    """
    Build an OData filter expression that allows documents accessible
    to any of the provided groups.

    Example output for groups ['team-data-engineering', 'all']:
      "allowed_groups/any(g: g eq 'team-data-engineering') or
       allowed_groups/any(g: g eq 'all')"
    """
    if not user_groups:
        # If no groups provided, only return publicly accessible documents
        return "allowed_groups/any(g: g eq 'all')"

    clauses = [f"allowed_groups/any(g: g eq '{g}')" for g in user_groups]
    return " or ".join(clauses)


def secure_search(
    search_client: SearchClient,
    query: str,
    user_groups: list[str],
    top: int = 5,
) -> list[dict]:
    """
    Execute a security-trimmed search query.

    Args:
        search_client: Authenticated Azure AI Search client.
        query: The natural-language search query.
        user_groups: List of group identifiers for the calling user,
                     retrieved from the identity provider at request time.
        top: Maximum number of results to return.

    Returns:
        List of matching documents, trimmed to those the caller may access.
    """
    security_filter = build_security_filter(user_groups)

    results = search_client.search(
        search_text=query,
        filter=security_filter,
        top=top,
        select=["id", "title", "space_key", "body_text", "allowed_groups"],
    )

    return [dict(r) for r in results]
```

---

## Step 4: Retrieve User Group Membership at Runtime

The security filter is only effective if it reflects the **actual groups** the calling user belongs to. In production, retrieve group membership from Microsoft Entra ID using the Microsoft Graph API.

```python
from azure.identity import DefaultAzureCredential
import requests


def get_user_groups(user_object_id: str) -> list[str]:
    """
    Retrieve the Entra group memberships for a user using Microsoft Graph.

    The caller must have 'GroupMember.Read.All' or 'Directory.Read.All'
    Graph API permission.

    Args:
        user_object_id: The Entra object ID (oid) of the authenticated user.

    Returns:
        List of group display names (or object IDs — use whichever matches
        what is stored in the 'allowed_groups' index field).
    """
    credential = DefaultAzureCredential()
    token = credential.get_token("https://graph.microsoft.com/.default").token

    resp = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{user_object_id}/getMemberObjects",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"securityEnabledOnly": False},
    )
    resp.raise_for_status()

    group_ids = resp.json().get("value", [])

    # Resolve display names if needed (optional — can use object IDs directly
    # if the index field stores object IDs instead of display names)
    return group_ids
```

> **Best practice:** For large organizations, prefer storing **Entra group object IDs** (GUIDs) in the `allowed_groups` field rather than display names. GUIDs are stable; display names can change.

---

## Script: `07_setup_security_filters.py`

This post-provision script demonstrates the full security filter setup:

1. Creates a new index schema that includes the `allowed_groups` field.
2. Pushes sample documents with security metadata into the index.
3. Runs a test query with and without the security filter to verify enforcement.

Run it after `06_create_agent.py`:

```bash
python scripts/postprovision/07_setup_security_filters.py
```

See [`scripts/postprovision/07_setup_security_filters.py`](../scripts/postprovision/07_setup_security_filters.py) for the full implementation.

---

## Upgrading to Native RBAC Scopes (Preview)

For data sources that use **Azure Data Lake Storage Gen2** (which is the backing store for Microsoft OneLake), Azure AI Search offers native POSIX-like ACL and RBAC scope enforcement. This is more powerful than string-based security filters because:

- Permission metadata is pulled **directly from ADLS Gen2 ACLs** during indexing — no manual tagging in the Gold layer.
- The identity check uses **Microsoft Entra tokens** rather than string comparison.
- Group membership resolution (including nested groups) is handled automatically by the service via Microsoft Graph.

### Prerequisites for Native RBAC (Preview)

- Azure AI Search REST API version `2025-11-01-preview` or a matching prerelease Azure SDK.
- OneLake files secured with [ADLS Gen2 access control](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control-model).
- Index created with `permissionFilterOption` enabled.
- Queries sent with the `x-ms-query-source-authorization` header carrying the user's Entra token.

### Enabling Permission Filters on the Index

```python
# Requires azure-search-documents prerelease package
# pip install azure-search-documents --pre

from azure.search.documents.indexes.models import SearchIndex

index = SearchIndex(
    name="confluence-secure-index",
    fields=[...],
    # Enable permission filter enforcement
    # (REST API: "permissionFilterOption": "enabled")
    # SDK property name may vary by prerelease version — check the changelog
)
```

### Query with User Token (Native RBAC)

```http
POST https://<search>.search.windows.net/indexes/<index>/docs/search?api-version=2025-11-01-preview
Authorization: Bearer <APP_SERVICE_API_KEY_OR_SEARCH_RBAC_TOKEN>
x-ms-query-source-authorization: Bearer <USER_ENTRA_TOKEN>
Content-Type: application/json

{
  "search": "What are the most discussed pages?"
}
```

Azure AI Search will:
1. Validate the caller's `Authorization` header against **Search Index Data Reader** role.
2. Extract user identity from `x-ms-query-source-authorization`.
3. Fetch group memberships via Microsoft Graph.
4. Construct a security filter internally and trim results before returning them.

> **Important behavior change (November 2025):** Starting with API version `2025-11-01-preview`, ACL-protected content is **not returned** if the user token is omitted — even if the request is authenticated with a valid service key. Omitting the user token now returns only publicly accessible documents. Use the `x-ms-enable-elevated-read: true` header (requires `Search Index Data Contributor`) for admin-level debugging.

---

## RBAC Roles Required

| Persona | Required Role | Scope |
|---|---|---|
| AI Search indexer (managed identity) | **Storage Blob Data Reader** | OneLake / ADLS Gen2 container |
| AI Search indexer (managed identity) | **Cognitive Services OpenAI User** | Azure OpenAI resource |
| Calling application | **Search Index Data Reader** | AI Search index |
| End-user token (RBAC enforcement) | Entra group membership matching document ACLs | N/A |
| Admin / developer debugging | **Search Index Data Contributor** | AI Search index |

These roles are assigned by `scripts/postprovision/04_setup_rbac.py`. The `Search Index Data Reader` role for the calling application must be added separately for each application identity that will query the index.

---

## ACL Entry Limits

| Data Source | Maximum ACL Entries per Document |
|---|---|
| ADLS Gen2 / OneLake | 32 entries per file or directory |
| SharePoint in Microsoft 365 | 1,000 entries per item |
| Security filter (string field) | No hard limit — governed by index field size |

For the string-filter approach used in this module, the `Collection(Edm.String)` field has no enforced entry count limit beyond the overall document size limit of 16 MB.

---

## Testing Security Enforcement

After running `07_setup_security_filters.py`, verify that the filter is working:

```bash
# Documents visible to team-data-engineering
curl -s -X POST \
  "https://<SEARCH_NAME>.search.windows.net/indexes/confluence-secure-index/docs/search?api-version=2024-07-01" \
  -H "api-key: <ADMIN_KEY>" \
  -H "Content-Type: application/json" \
  -d '{
    "search": "*",
    "filter": "allowed_groups/any(g: g eq '"'"'team-data-engineering'"'"') or allowed_groups/any(g: g eq '"'"'all'"'"')",
    "select": "id, title, allowed_groups"
  }'

# Documents visible only to team-finance (should be a subset)
curl -s -X POST \
  "https://<SEARCH_NAME>.search.windows.net/indexes/confluence-secure-index/docs/search?api-version=2024-07-01" \
  -H "api-key: <ADMIN_KEY>" \
  -H "Content-Type: application/json" \
  -d '{
    "search": "*",
    "filter": "allowed_groups/any(g: g eq '"'"'team-finance'"'"')",
    "select": "id, title, allowed_groups"
  }'
```

Expected: the `team-finance` query returns only documents explicitly tagged for `team-finance`, not documents tagged `team-data-engineering`.

---

## Summary

| Step | What It Does | Where |
|---|---|---|
| 1. ETL Gold layer | Adds `allowed_groups` column to CSV output | `src/etl/gold/confluence_aggregation.py` |
| 2. Index schema | Declares `allowed_groups` as filterable field | `scripts/postprovision/07_setup_security_filters.py` |
| 3. Query time | Injects OData filter from caller's group membership | `src/ai_agent/analyst.py` |
| 4. Identity resolution | Retrieves Entra groups from Microsoft Graph | App-layer, shown in code samples above |
| 5. (Preview) Native RBAC | Entra token in `x-ms-query-source-authorization` header | Upgrade path documented above |

The security-filter approach implemented here is **API-version agnostic** (works with all GA SDK versions) and gives immediate, predictable enforcement. As the native ACL/RBAC preview matures, the project can migrate to token-based enforcement with minimal changes to the query layer.
