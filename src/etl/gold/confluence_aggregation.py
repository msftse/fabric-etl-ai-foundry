"""
Gold Layer — Confluence Business-Ready Aggregations.

Produces curated, analysis-ready datasets from silver-layer Confluence data:
  1. content_by_space      — Page count, total words, avg word count per space
  2. author_activity       — Pages created, comments made, total words per author
  3. content_timeline      — Pages and comments created per day/week
  4. most_discussed_pages  — Pages ranked by number of comments
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from src.onelake.client import OneLakeClient
from src.utils.logging import get_logger

log = get_logger(__name__)


class ConfluenceGoldAggregation:
    """Build gold-tier aggregations from silver Confluence data."""

    def __init__(self, onelake: OneLakeClient) -> None:
        self._onelake = onelake

    def build_all(
        self, silver_data: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """Run all gold aggregations and return a dict of table_name -> DataFrame."""
        log.info("confluence_gold_build_start")

        pages_df = silver_data.get("confluence_pages", pd.DataFrame())
        comments_df = silver_data.get("confluence_comments", pd.DataFrame())

        results = {}

        if not pages_df.empty:
            results["confluence_content_by_space"] = self._content_by_space(pages_df)
            results["confluence_author_activity"] = self._author_activity(
                pages_df, comments_df
            )
            results["confluence_most_discussed"] = self._most_discussed_pages(
                pages_df, comments_df
            )

        if not pages_df.empty or not comments_df.empty:
            results["confluence_content_timeline"] = self._content_timeline(
                pages_df, comments_df
            )

        # Persist to gold layer (Parquet for analytics + CSV for AI Search indexing)
        for name, df in results.items():
            self._onelake.write_parquet(df, layer="gold", table=name)
            self._onelake.write_csv(df, layer="gold", table=name)

        log.info("confluence_gold_build_done", tables=list(results.keys()))
        return results

    # ── Aggregation helpers ──────────────────────────────────────────

    def _content_by_space(self, pages_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate content metrics per Confluence space."""
        agg = (
            pages_df.groupby("space_key")
            .agg(
                page_count=("page_id", "nunique"),
                total_words=("word_count", "sum"),
                avg_word_count=("word_count", "mean"),
                total_content_length=("content_length", "sum"),
                latest_update=("last_updated_date", "max"),
            )
            .reset_index()
            .sort_values("page_count", ascending=False)
        )
        agg["avg_word_count"] = agg["avg_word_count"].round(0).astype(int)
        agg["_aggregated_at"] = datetime.now(timezone.utc).isoformat()
        log.info("confluence_gold_content_by_space", spaces=len(agg))
        return agg

    def _author_activity(
        self, pages_df: pd.DataFrame, comments_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Aggregate activity metrics per author."""
        # Page authorship
        page_agg = (
            pages_df.groupby("created_by")
            .agg(
                pages_created=("page_id", "nunique"),
                pages_total_words=("word_count", "sum"),
            )
            .reset_index()
            .rename(columns={"created_by": "author"})
        )

        # Comment activity
        if not comments_df.empty:
            comment_agg = (
                comments_df.groupby("author")
                .agg(
                    comments_made=("comment_id", "nunique"),
                    comments_total_words=("word_count", "sum"),
                )
                .reset_index()
            )
            agg = page_agg.merge(comment_agg, on="author", how="outer").fillna(0)
        else:
            agg = page_agg.copy()
            agg["comments_made"] = 0
            agg["comments_total_words"] = 0

        # Ensure integer types
        for col in [
            "pages_created",
            "pages_total_words",
            "comments_made",
            "comments_total_words",
        ]:
            agg[col] = agg[col].astype(int)

        agg["total_contributions"] = agg["pages_created"] + agg["comments_made"]
        agg = agg.sort_values("total_contributions", ascending=False)
        agg["_aggregated_at"] = datetime.now(timezone.utc).isoformat()

        log.info("confluence_gold_author_activity", authors=len(agg))
        return agg

    def _content_timeline(
        self, pages_df: pd.DataFrame, comments_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Aggregate content creation by day."""
        rows = []

        # Pages by day
        if not pages_df.empty and "created_date" in pages_df.columns:
            page_daily = pages_df.copy()
            page_daily["day"] = page_daily["created_date"].dt.date
            page_counts = (
                page_daily.groupby("day")
                .agg(pages_created=("page_id", "nunique"))
                .reset_index()
            )
            rows.append(page_counts)

        # Comments by day
        if not comments_df.empty and "created_date" in comments_df.columns:
            comment_daily = comments_df.copy()
            comment_daily["day"] = comment_daily["created_date"].dt.date
            comment_counts = (
                comment_daily.groupby("day")
                .agg(comments_created=("comment_id", "nunique"))
                .reset_index()
            )
            rows.append(comment_counts)

        if not rows:
            return pd.DataFrame()

        # Merge page and comment timelines
        if len(rows) == 2:
            agg = rows[0].merge(rows[1], on="day", how="outer").fillna(0)
        else:
            agg = rows[0]
            if "pages_created" not in agg.columns:
                agg["pages_created"] = 0
            if "comments_created" not in agg.columns:
                agg["comments_created"] = 0

        agg = agg.sort_values("day")
        for col in ["pages_created", "comments_created"]:
            if col in agg.columns:
                agg[col] = agg[col].astype(int)

        agg["total_activity"] = agg.get("pages_created", 0) + agg.get(
            "comments_created", 0
        )
        agg["cumulative_pages"] = agg.get("pages_created", pd.Series([0])).cumsum()
        agg["_aggregated_at"] = datetime.now(timezone.utc).isoformat()

        log.info("confluence_gold_content_timeline", days=len(agg))
        return agg

    def _most_discussed_pages(
        self, pages_df: pd.DataFrame, comments_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Rank pages by number of comments."""
        if comments_df.empty:
            agg = pages_df[["page_id", "title", "space_key", "word_count"]].copy()
            agg["comment_count"] = 0
        else:
            comment_counts = (
                comments_df.groupby("page_id")
                .agg(comment_count=("comment_id", "nunique"))
                .reset_index()
            )
            # Ensure page_id types match for merge
            comment_counts["page_id"] = comment_counts["page_id"].astype(str)
            pages_merge = pages_df.copy()
            pages_merge["page_id"] = pages_merge["page_id"].astype(str)

            agg = pages_merge[["page_id", "title", "space_key", "word_count"]].merge(
                comment_counts, on="page_id", how="left"
            )
            agg["comment_count"] = agg["comment_count"].fillna(0).astype(int)

        agg = agg.sort_values("comment_count", ascending=False)
        agg["_aggregated_at"] = datetime.now(timezone.utc).isoformat()

        log.info("confluence_gold_most_discussed", pages=len(agg))
        return agg
