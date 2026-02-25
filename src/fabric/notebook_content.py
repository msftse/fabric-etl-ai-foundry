"""
Fabric Notebook Content Loader.

Loads .ipynb notebook files from the ``notebooks/`` directory, injects
dynamic values (lakehouse ID, workspace ID, lakehouse name) via placeholder
token replacement, and returns base64-encoded payloads ready for the
Fabric REST API.

Placeholder tokens used in the .ipynb files:
    {{LAKEHOUSE_ID}}   — Fabric lakehouse item ID
    {{WORKSPACE_ID}}   — Fabric workspace ID
    {{LAKEHOUSE_NAME}} — Lakehouse display name (e.g. "confluencelakehouse")
"""

from __future__ import annotations

import base64
from pathlib import Path

NOTEBOOKS_DIR = Path(__file__).resolve().parent.parent.parent / "notebooks"


def _load_and_inject(
    filename: str,
    lakehouse_id: str,
    workspace_id: str,
    lakehouse_name: str = "confluencelakehouse",
) -> str:
    """
    Read an .ipynb file from the notebooks/ directory, replace placeholder
    tokens with actual values, and return the base64-encoded payload.
    """
    notebook_path = NOTEBOOKS_DIR / filename
    if not notebook_path.exists():
        raise FileNotFoundError(
            f"Notebook not found: {notebook_path}  (looked in {NOTEBOOKS_DIR})"
        )

    raw = notebook_path.read_text(encoding="utf-8")

    # Inject dynamic values
    raw = raw.replace("{{LAKEHOUSE_ID}}", lakehouse_id)
    raw = raw.replace("{{WORKSPACE_ID}}", workspace_id)
    raw = raw.replace("{{LAKEHOUSE_NAME}}", lakehouse_name)

    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


# ── Public helpers (same signature as the old generators) ────────────


def bronze_notebook(lakehouse_id: str, workspace_id: str) -> str:
    """Load and return the Bronze notebook payload (base64)."""
    return _load_and_inject(
        "bronze_confluence_extract.ipynb", lakehouse_id, workspace_id
    )


def silver_notebook(lakehouse_id: str, workspace_id: str) -> str:
    """Load and return the Silver notebook payload (base64)."""
    return _load_and_inject(
        "silver_confluence_transform.ipynb", lakehouse_id, workspace_id
    )


def gold_notebook(lakehouse_id: str, workspace_id: str) -> str:
    """Load and return the Gold notebook payload (base64)."""
    return _load_and_inject(
        "gold_confluence_aggregation.ipynb", lakehouse_id, workspace_id
    )
