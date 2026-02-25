"""
Silver Layer — Confluence Data Transformation.

Cleanses and enriches raw Confluence data from the bronze layer:
  1. Strips HTML from page/comment bodies to extract plain text
  2. Parses and normalizes dates
  3. Computes derived columns (word count, content length)
  4. Standardizes text fields
  5. Removes ingestion metadata columns
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from bs4 import BeautifulSoup

from src.onelake.client import OneLakeClient
from src.utils.logging import get_logger

log = get_logger(__name__)


def _html_to_text(html: str) -> str:
    """Strip HTML tags and return clean text."""
    if not html or not isinstance(html, str):
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator=" ", strip=True)


class ConfluenceSilverTransform:
    """Cleanse and enrich Confluence bronze data for the silver layer."""

    def __init__(self, onelake: OneLakeClient) -> None:
        self._onelake = onelake

    def transform_all(
        self, bronze_data: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """
        Transform all Confluence bronze tables to silver.

        Returns a dict of table_name -> cleaned DataFrame.
        """
        log.info("confluence_silver_transform_start")

        result = {}

        # Spaces
        if (
            "confluence_spaces" in bronze_data
            and not bronze_data["confluence_spaces"].empty
        ):
            result["confluence_spaces"] = self._transform_spaces(
                bronze_data["confluence_spaces"]
            )

        # Pages
        if (
            "confluence_pages" in bronze_data
            and not bronze_data["confluence_pages"].empty
        ):
            result["confluence_pages"] = self._transform_pages(
                bronze_data["confluence_pages"]
            )

        # Comments
        if (
            "confluence_comments" in bronze_data
            and not bronze_data["confluence_comments"].empty
        ):
            result["confluence_comments"] = self._transform_comments(
                bronze_data["confluence_comments"]
            )

        # Persist all to silver (Parquet + CSV for AI Search indexing)
        for table_name, df in result.items():
            self._onelake.write_parquet(df, layer="silver", table=table_name)
            self._onelake.write_csv(df, layer="silver", table=table_name)
            log.info("confluence_silver_written", table=table_name, rows=len(df))

        log.info("confluence_silver_transform_done", tables=list(result.keys()))
        return result

    def _transform_spaces(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean spaces data."""
        log.info("confluence_silver_spaces_start", rows=len(df))
        out = df.copy()

        # Drop ingestion metadata
        meta_cols = [c for c in out.columns if c.startswith("_")]
        out = out.drop(columns=meta_cols, errors="ignore")

        # Standardize
        out["space_name"] = out["space_name"].str.strip()
        out["space_key"] = out["space_key"].str.upper().str.strip()
        out["space_type"] = out["space_type"].str.lower().str.strip()

        # Clean description
        out["description"] = out["description"].apply(_html_to_text)

        # Audit
        out["_processed_at"] = datetime.now(timezone.utc).isoformat()

        log.info("confluence_silver_spaces_done", rows=len(out))
        return out

    def _transform_pages(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and enrich pages data."""
        log.info("confluence_silver_pages_start", rows=len(df))
        out = df.copy()

        # Drop ingestion metadata
        meta_cols = [c for c in out.columns if c.startswith("_")]
        out = out.drop(columns=meta_cols, errors="ignore")

        # Extract plain text from HTML body
        out["body_text"] = out["body_html"].apply(_html_to_text)

        # Derived columns
        out["word_count"] = out["body_text"].apply(
            lambda x: len(x.split()) if isinstance(x, str) else 0
        )
        out["content_length"] = out["body_text"].str.len().fillna(0).astype(int)

        # Parse dates
        out["created_date"] = pd.to_datetime(out["created_date"], errors="coerce")
        out["last_updated_date"] = pd.to_datetime(
            out["last_updated_date"], errors="coerce"
        )

        # Standardize
        out["title"] = out["title"].str.strip()
        out["space_key"] = out["space_key"].str.upper().str.strip()
        out["status"] = out["status"].str.lower().str.strip()

        # Deduplicate by page_id
        before = len(out)
        out = out.drop_duplicates(subset=["page_id"], keep="last")
        log.info("confluence_silver_pages_dedup", removed=before - len(out))

        # Audit
        out["_processed_at"] = datetime.now(timezone.utc).isoformat()

        log.info("confluence_silver_pages_done", rows=len(out))
        return out

    def _transform_comments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and enrich comments data."""
        log.info("confluence_silver_comments_start", rows=len(df))
        out = df.copy()

        # Drop ingestion metadata
        meta_cols = [c for c in out.columns if c.startswith("_")]
        out = out.drop(columns=meta_cols, errors="ignore")

        # Extract plain text from HTML
        out["comment_text"] = out["body_html"].apply(_html_to_text)

        # Derived columns
        out["word_count"] = out["comment_text"].apply(
            lambda x: len(x.split()) if isinstance(x, str) else 0
        )

        # Parse dates
        out["created_date"] = pd.to_datetime(out["created_date"], errors="coerce")

        # Standardize
        out["author"] = out["author"].str.strip()

        # Deduplicate by comment_id
        before = len(out)
        out = out.drop_duplicates(subset=["comment_id"], keep="last")
        log.info("confluence_silver_comments_dedup", removed=before - len(out))

        # Audit
        out["_processed_at"] = datetime.now(timezone.utc).isoformat()

        log.info("confluence_silver_comments_done", rows=len(out))
        return out
