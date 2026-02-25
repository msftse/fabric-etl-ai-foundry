"""
Confluence Cloud Client.

Wraps the atlassian-python-api library to provide:
  - Connection to Confluence Cloud
  - Extraction of all spaces, pages, and comments
  - Returns data as pandas DataFrames ready for bronze ingestion
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from atlassian import Confluence

from config.settings import ConfluenceConfig
from src.utils.logging import get_logger

log = get_logger(__name__)


class ConfluenceClient:
    """Extract data from Confluence Cloud via the REST API."""

    def __init__(self, config: ConfluenceConfig) -> None:
        if not config.is_configured:
            raise ValueError(
                "Confluence is not configured. Set CONFLUENCE_URL, "
                "CONFLUENCE_EMAIL, and CONFLUENCE_API_TOKEN in your .env file."
            )
        self._config = config
        self._api = Confluence(
            url=config.url,
            username=config.email,
            password=config.api_token,
            cloud=True,
        )
        log.info("confluence_client_init", url=config.url)

    # ── Spaces ───────────────────────────────────────────────────────

    def get_all_spaces(self) -> pd.DataFrame:
        """Fetch all Confluence spaces and return as a DataFrame."""
        log.info("confluence_fetch_spaces_start")
        spaces = []
        start = 0
        limit = 50

        while True:
            batch = self._api.get_all_spaces(
                start=start, limit=limit, expand="description.plain"
            )
            results = batch.get("results", [])
            if not results:
                break

            for s in results:
                spaces.append(
                    {
                        "space_id": s.get("id"),
                        "space_key": s.get("key"),
                        "space_name": s.get("name"),
                        "space_type": s.get("type"),
                        "description": (
                            s.get("description", {}).get("plain", {}).get("value", "")
                        ),
                    }
                )

            if batch.get("size", 0) < limit:
                break
            start += limit

        log.info("confluence_fetch_spaces_done", count=len(spaces))
        return pd.DataFrame(spaces)

    # ── Pages ────────────────────────────────────────────────────────

    def get_all_pages(self, space_key: str | None = None) -> pd.DataFrame:
        """
        Fetch all pages, optionally filtered by space key.
        If space_key is None, fetches pages from all spaces.
        Returns a DataFrame with page metadata and body content.
        """
        log.info("confluence_fetch_pages_start", space_key=space_key or "ALL")

        if space_key:
            space_keys = [space_key]
        else:
            spaces_df = self.get_all_spaces()
            space_keys = spaces_df["space_key"].tolist()

        pages = []
        for sk in space_keys:
            log.info("confluence_fetch_pages_space", space_key=sk)
            start = 0
            limit = 50

            while True:
                results = self._api.get_all_pages_from_space(
                    sk,
                    start=start,
                    limit=limit,
                    expand="body.storage,version,history",
                )
                if not results:
                    break

                for page in results:
                    body_html = page.get("body", {}).get("storage", {}).get("value", "")
                    version_info = page.get("version", {})
                    history_info = page.get("history", {})

                    pages.append(
                        {
                            "page_id": page.get("id"),
                            "space_key": sk,
                            "title": page.get("title"),
                            "status": page.get("status"),
                            "body_html": body_html,
                            "version_number": version_info.get("number"),
                            "created_by": (
                                history_info.get("createdBy", {}).get("displayName", "")
                            ),
                            "created_date": history_info.get("createdDate", ""),
                            "last_updated_by": (
                                version_info.get("by", {}).get("displayName", "")
                            ),
                            "last_updated_date": version_info.get("when", ""),
                        }
                    )

                if len(results) < limit:
                    break
                start += limit

        log.info("confluence_fetch_pages_done", count=len(pages))
        return pd.DataFrame(pages)

    # ── Comments ─────────────────────────────────────────────────────

    def get_comments_for_page(self, page_id: str) -> list[dict]:
        """Fetch all comments for a single page."""
        comments = []
        start = 0
        limit = 50

        while True:
            result = self._api.get_page_comments(
                page_id,
                start=start,
                limit=limit,
                expand="body.storage,version",
            )
            items = result.get("results", [])
            if not items:
                break

            for c in items:
                comments.append(
                    {
                        "comment_id": c.get("id"),
                        "page_id": page_id,
                        "body_html": (
                            c.get("body", {}).get("storage", {}).get("value", "")
                        ),
                        "author": (
                            c.get("version", {}).get("by", {}).get("displayName", "")
                        ),
                        "created_date": c.get("version", {}).get("when", ""),
                    }
                )

            if len(items) < limit:
                break
            start += limit

        return comments

    def get_all_comments(self, pages_df: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        Fetch comments for all pages.
        If pages_df is provided, uses the page_id column from it.
        Otherwise, fetches all pages first.
        """
        if pages_df is None:
            pages_df = self.get_all_pages()

        if pages_df.empty:
            log.info("confluence_fetch_comments_skip", reason="no pages")
            return pd.DataFrame()

        log.info("confluence_fetch_comments_start", pages=len(pages_df))
        all_comments = []

        for page_id in pages_df["page_id"]:
            page_comments = self.get_comments_for_page(str(page_id))
            all_comments.extend(page_comments)

        log.info("confluence_fetch_comments_done", count=len(all_comments))
        return pd.DataFrame(all_comments) if all_comments else pd.DataFrame()

    # ── Full Extraction ──────────────────────────────────────────────

    def extract_all(self) -> dict[str, pd.DataFrame]:
        """
        Extract all data from Confluence: spaces, pages, and comments.
        Returns a dict of table_name -> DataFrame.
        """
        log.info("confluence_extract_all_start")

        spaces_df = self.get_all_spaces()
        pages_df = self.get_all_pages()
        comments_df = self.get_all_comments(pages_df)

        result = {
            "confluence_spaces": spaces_df,
            "confluence_pages": pages_df,
            "confluence_comments": comments_df,
        }

        log.info(
            "confluence_extract_all_done",
            spaces=len(spaces_df),
            pages=len(pages_df),
            comments=len(comments_df),
        )
        return result
