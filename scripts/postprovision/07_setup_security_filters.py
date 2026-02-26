#!/usr/bin/env python3
"""
07_setup_security_filters.py — Demonstrate document-level access control
for the Confluence AI Search index using security filters.

This script implements the Security Filters (string comparison) pattern
described in:
  https://learn.microsoft.com/en-us/azure/search/search-document-level-access-overview
  https://learn.microsoft.com/en-us/azure/search/search-query-access-control-rbac-enforcement

What it does:
  1. Creates a standalone AI Search index with an 'allowed_groups' field
     (Collection(Edm.String), filterable) alongside standard content fields.
  2. Pushes sample Confluence-style documents, each tagged with one or more
     group identifiers that are allowed to see that document.
  3. Runs test queries with different security filters to verify that results
     are correctly trimmed based on the caller's group membership.
  4. Prints a summary showing which documents are visible to each group.

Run after 06_create_agent.py:
    python scripts/postprovision/07_setup_security_filters.py

The index created here is named 'confluence-secure-demo' and is separate
from the main knowledge-source index created in 05_setup_search_index.py.
It is intended as an educational demo. See docs/security-index-access.md
for the full module write-up and upgrade path to native RBAC enforcement.
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from _helpers import load_azd_env, require_env

from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchField,
    SearchFieldDataType,
    SimpleField,
    SearchableField,
)
from azure.mgmt.search import SearchManagementClient
from azure.identity import DefaultAzureCredential

# ── Constants ─────────────────────────────────────────────────────────────────

DEMO_INDEX_NAME = "confluence-secure-demo"

# Sample space-to-group mapping that mirrors what a Gold-layer ETL step would
# stamp onto each document. In production, derive this from your identity
# provider (e.g. Entra ID group object IDs).
SPACE_GROUP_MAP: dict[str, list[str]] = {
    "ETLDEMO": ["team-data-engineering", "all"],
    "PLAT": ["team-platform", "all"],
    "FIN": ["team-finance"],
    "HR": ["team-hr"],
}

# Sample documents representing Gold-layer Confluence data.
# The 'allowed_groups' field is derived from SPACE_GROUP_MAP so the mapping
# stays consistent — any update to SPACE_GROUP_MAP is automatically reflected
# in the sample data without manual edits.
_RAW_DOCUMENTS = [
    {
        "id": "page-001",
        "title": "ETL Pipeline Architecture",
        "space_key": "ETLDEMO",
        "body_text": (
            "This page describes the Bronze-Silver-Gold medallion architecture "
            "used in the Confluence ETL project."
        ),
        "author": "alice@contoso.com",
        "word_count": 120,
    },
    {
        "id": "page-002",
        "title": "Platform Infrastructure Overview",
        "space_key": "PLAT",
        "body_text": (
            "Overview of the Azure infrastructure: Fabric Capacity, AI Search, "
            "AI Foundry Hub, and OneLake layout."
        ),
        "author": "bob@contoso.com",
        "word_count": 95,
    },
    {
        "id": "page-003",
        "title": "Q3 Finance Review",
        "space_key": "FIN",
        "body_text": (
            "Confidential quarterly finance review: revenue breakdown by region "
            "and product line. Internal use only."
        ),
        "author": "carol@contoso.com",
        "word_count": 210,
    },
    {
        "id": "page-004",
        "title": "HR Onboarding Handbook",
        "space_key": "HR",
        "body_text": (
            "HR onboarding process, benefits information, and company policies "
            "for new hires."
        ),
        "author": "dave@contoso.com",
        "word_count": 450,
    },
    {
        "id": "page-005",
        "title": "Cross-Team Data Standards",
        "space_key": "ETLDEMO",
        "body_text": (
            "Standards for naming conventions, data types, and schema evolution "
            "across all teams."
        ),
        "author": "alice@contoso.com",
        "word_count": 80,
    },
]

# Stamp each document with its allowed_groups derived from SPACE_GROUP_MAP.
# Spaces not listed in the map default to ["all"] (publicly accessible).
SAMPLE_DOCUMENTS = [
    {**doc, "allowed_groups": SPACE_GROUP_MAP.get(doc["space_key"], ["all"])}
    for doc in _RAW_DOCUMENTS
]

# Test scenarios: each entry defines a simulated user and their group membership.
# Expected visibility is derived at runtime from SAMPLE_DOCUMENTS in run_security_test().
TEST_SCENARIOS = [
    {
        "user": "alice (team-data-engineering)",
        "groups": ["team-data-engineering"],
        # Effective groups (after adding 'all'): ['team-data-engineering', 'all']
        # Sees: page-001 (team-data-engineering), page-002 (all), page-005 (team-data-engineering + all)
    },
    {
        "user": "carol (team-finance)",
        "groups": ["team-finance"],
        # Effective groups (after adding 'all'): ['team-finance', 'all']
        # Sees: page-001 (all), page-002 (all), page-003 (team-finance), page-005 (all)
    },
    {
        "user": "eve (no group — public only)",
        "groups": ["all"],
        # Effective groups: ['all']
        # Sees: page-001 (all), page-002 (all), page-005 (all)
    },
]


# ── Index Schema ──────────────────────────────────────────────────────────────


def build_index_schema() -> SearchIndex:
    """
    Build an AI Search index schema that includes the 'allowed_groups'
    security field alongside standard Confluence content fields.

    The 'allowed_groups' field is declared as:
      - type: Collection(Edm.String)  — stores multiple group identifiers
      - filterable: True              — required for OData $filter expressions
      - retrievable: True             — return in results for verification
      - searchable: False             — not needed for full-text search
    """
    fields = [
        SimpleField(
            name="id",
            type=SearchFieldDataType.String,
            key=True,
            filterable=True,
        ),
        SearchableField(
            name="title",
            type=SearchFieldDataType.String,
            filterable=True,
            sortable=True,
        ),
        SimpleField(
            name="space_key",
            type=SearchFieldDataType.String,
            filterable=True,
            sortable=True,
        ),
        SearchableField(
            name="body_text",
            type=SearchFieldDataType.String,
        ),
        SimpleField(
            name="author",
            type=SearchFieldDataType.String,
            filterable=True,
        ),
        SimpleField(
            name="word_count",
            type=SearchFieldDataType.Int32,
            filterable=True,
            sortable=True,
        ),
        # ── Security field ─────────────────────────────────────────────────
        # Collection(Edm.String) with filterable=True allows the OData expression:
        #   allowed_groups/any(g: g eq 'team-data-engineering')
        # This is the key field that enables security trimming.
        SearchField(
            name="allowed_groups",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            filterable=True,
            retrievable=True,
            searchable=False,
        ),
    ]

    return SearchIndex(name=DEMO_INDEX_NAME, fields=fields)


# ── Security Filter Builder ───────────────────────────────────────────────────


def build_security_filter(user_groups: list[str]) -> str:
    """
    Build an OData filter expression that returns documents accessible
    to any of the provided group identifiers.

    The expression uses the Collection 'any' lambda to match documents
    where at least one entry in 'allowed_groups' equals a group the
    caller belongs to.

    Example output for groups ['team-data-engineering', 'all']:
        "allowed_groups/any(g: g eq 'team-data-engineering') or
         allowed_groups/any(g: g eq 'all')"

    An empty group list defaults to 'all' (public documents only).

    Reference:
        https://learn.microsoft.com/en-us/azure/search/search-query-odata-collection-operators
    """
    if not user_groups:
        return "allowed_groups/any(g: g eq 'all')"

    # Sanitize: escape single quotes to prevent OData injection (two single quotes '' escape a single quote ' in OData)
    safe_groups = [g.replace("'", "''") for g in user_groups]
    clauses = [f"allowed_groups/any(g: g eq '{g}')" for g in safe_groups]
    return " or ".join(clauses)


# ── Test Queries ──────────────────────────────────────────────────────────────


def run_security_test(
    search_client: SearchClient,
    scenario: dict,
) -> tuple[set[str], bool]:
    """
    Run a search query with the security filter applied for a given scenario.

    Expected document IDs are derived at runtime by computing the overlap
    between each document's 'allowed_groups' in SAMPLE_DOCUMENTS and the
    scenario's effective_groups (user groups plus 'all').

    Returns (actual_ids, passed) where passed is True if the actual result
    set matches the expected set derived from SAMPLE_DOCUMENTS.
    """
    groups = scenario["groups"]

    # Always add 'all' so publicly-tagged documents are visible to everyone
    effective_groups = list(set(groups + ["all"]))
    security_filter = build_security_filter(effective_groups)

    results = search_client.search(
        search_text="*",
        filter=security_filter,
        select=["id", "title", "space_key", "allowed_groups"],
        top=50,
    )

    actual_ids = set()
    result_rows = []
    for r in results:
        actual_ids.add(r["id"])
        result_rows.append(
            f"      [{r['id']}] {r['title']} "
            f"(space: {r['space_key']}, groups: {r.get('allowed_groups', [])})"
        )

    # Derive expected: documents where allowed_groups overlaps effective_groups
    expected_ids = {
        doc["id"]
        for doc in SAMPLE_DOCUMENTS
        if set(doc["allowed_groups"]) & set(effective_groups)
    }

    passed = actual_ids == expected_ids

    print(f"\n  User: {scenario['user']}")
    print(f"  Effective groups: {effective_groups}")
    print(f"  OData filter: {security_filter}")
    print(f"  Results ({len(actual_ids)} documents):")
    for row in result_rows:
        print(row)
    if passed:
        print(f"  PASS — returned exactly the {len(actual_ids)} expected document(s)")
    else:
        missing = expected_ids - actual_ids
        extra = actual_ids - expected_ids
        print(f"  FAIL — expected {expected_ids}")
        if missing:
            print(f"         Missing: {missing}")
        if extra:
            print(f"         Unexpected: {extra}")

    return actual_ids, passed


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    env = load_azd_env()
    search_name = require_env(env, "AI_SEARCH_SERVICE_NAME")
    subscription_id = require_env(env, "AZURE_SUBSCRIPTION_ID")
    resource_group = require_env(env, "AZURE_RESOURCE_GROUP")

    search_url = f"https://{search_name}.search.windows.net"

    # Obtain admin key via Azure Management API (same pattern as script 05)
    credential = DefaultAzureCredential()
    mgmt_client = SearchManagementClient(credential, subscription_id)
    keys = mgmt_client.admin_keys.get(resource_group, search_name)
    admin_key = keys.primary_key

    index_client = SearchIndexClient(
        endpoint=search_url,
        credential=AzureKeyCredential(admin_key),
    )

    # ── 1. Create (or recreate) the demo index ─────────────────────────────
    print(f"\n[07] Setting up security filter demo index '{DEMO_INDEX_NAME}'...")

    try:
        index_client.delete_index(DEMO_INDEX_NAME)
        print(f"  Deleted existing index '{DEMO_INDEX_NAME}'.")
    except ResourceNotFoundError:
        pass  # Index does not exist yet — that is fine

    schema = build_index_schema()
    index_client.create_index(schema)
    print(f"  Created index '{DEMO_INDEX_NAME}' with 'allowed_groups' security field.")

    # ── 2. Upload sample documents ─────────────────────────────────────────
    print(f"\n  Uploading {len(SAMPLE_DOCUMENTS)} sample documents...")
    search_client = SearchClient(
        endpoint=search_url,
        index_name=DEMO_INDEX_NAME,
        credential=AzureKeyCredential(admin_key),
    )
    result = search_client.upload_documents(documents=SAMPLE_DOCUMENTS)
    succeeded = sum(1 for r in result if r.succeeded)
    print(f"  Uploaded: {succeeded}/{len(SAMPLE_DOCUMENTS)} documents succeeded.")

    # AI Search indexing is near-real-time; allow a brief propagation delay
    # before running test queries.
    print("  Waiting 5 seconds for index propagation...")
    time.sleep(5)

    # ── 3. Run security filter test scenarios ─────────────────────────────
    print("\n  Running security filter test scenarios...")
    print("  " + "─" * 60)

    all_passed = True
    for scenario in TEST_SCENARIOS:
        _, passed = run_security_test(search_client, scenario)
        if not passed:
            all_passed = False

    # ── 4. Summary ─────────────────────────────────────────────────────────
    print("\n  " + "─" * 60)
    if all_passed:
        print("  All security filter tests PASSED.")
    else:
        print("  Some security filter tests FAILED — review results above.")

    print("\n  Key takeaways:")
    print("    1. The 'allowed_groups' field (Collection(Edm.String), filterable)")
    print("       is the security boundary for this approach.")
    print("    2. The OData 'any' lambda checks document-level group membership.")
    print("    3. Always include 'all' in the effective groups so publicly-tagged")
    print("       documents are visible to every authenticated user.")
    print("    4. In production, derive group membership from Entra ID at request")
    print("       time — never from a client-supplied header.")
    print()
    print("  For native RBAC enforcement (preview) using x-ms-query-source-authorization,")
    print("  see: docs/security-index-access.md#upgrading-to-native-rbac-scopes")
    print()
    print(f"  Demo index: {search_url}/indexes/{DEMO_INDEX_NAME}")
    print("  Done.")


if __name__ == "__main__":
    main()
