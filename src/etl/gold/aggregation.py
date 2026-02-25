"""
Gold Layer — Business-Ready Aggregations.

Produces curated, analysis-ready datasets from silver-layer data:
  1. revenue_by_country   — Total revenue per shipping country
  2. revenue_by_category  — Total revenue and order count per product category
  3. daily_revenue        — Revenue aggregated by day
  4. top_customers        — Top customers by total spend
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from src.onelake.client import OneLakeClient
from src.utils.logging import get_logger

log = get_logger(__name__)


class GoldAggregation:
    """Build gold-tier aggregations from silver data and persist to OneLake."""

    def __init__(self, onelake: OneLakeClient) -> None:
        self._onelake = onelake

    def build_all(self, silver_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """Run all gold aggregations and return a dict of table_name -> DataFrame."""
        log.info("gold_build_start", input_rows=len(silver_df))

        results = {
            "revenue_by_country": self._revenue_by_country(silver_df),
            "revenue_by_category": self._revenue_by_category(silver_df),
            "daily_revenue": self._daily_revenue(silver_df),
            "top_customers": self._top_customers(silver_df),
        }

        for name, df in results.items():
            self._onelake.write_parquet(df, layer="gold", table=name)

        log.info("gold_build_done", tables=list(results.keys()))
        return results

    # ── Aggregation helpers ──────────────────────────────────────────

    def _revenue_by_country(self, df: pd.DataFrame) -> pd.DataFrame:
        agg = (
            df.groupby("shipping_country")
            .agg(
                total_revenue=("total_amount", "sum"),
                order_count=("order_id", "nunique"),
                avg_order_value=("total_amount", "mean"),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
        )
        agg["total_revenue"] = agg["total_revenue"].round(2)
        agg["avg_order_value"] = agg["avg_order_value"].round(2)
        agg["_aggregated_at"] = datetime.now(timezone.utc).isoformat()
        log.info("gold_revenue_by_country", countries=len(agg))
        return agg

    def _revenue_by_category(self, df: pd.DataFrame) -> pd.DataFrame:
        agg = (
            df.groupby("category")
            .agg(
                total_revenue=("total_amount", "sum"),
                order_count=("order_id", "nunique"),
                total_units=("quantity", "sum"),
                avg_unit_price=("unit_price", "mean"),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
        )
        agg["total_revenue"] = agg["total_revenue"].round(2)
        agg["avg_unit_price"] = agg["avg_unit_price"].round(2)
        agg["_aggregated_at"] = datetime.now(timezone.utc).isoformat()
        log.info("gold_revenue_by_category", categories=len(agg))
        return agg

    def _daily_revenue(self, df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        df_copy["order_day"] = df_copy["order_date"].dt.date
        agg = (
            df_copy.groupby("order_day")
            .agg(
                daily_revenue=("total_amount", "sum"),
                order_count=("order_id", "nunique"),
            )
            .reset_index()
            .sort_values("order_day")
        )
        agg["daily_revenue"] = agg["daily_revenue"].round(2)
        agg["cumulative_revenue"] = agg["daily_revenue"].cumsum().round(2)
        agg["_aggregated_at"] = datetime.now(timezone.utc).isoformat()
        log.info("gold_daily_revenue", days=len(agg))
        return agg

    def _top_customers(self, df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        agg = (
            df.groupby("customer_id")
            .agg(
                total_spend=("total_amount", "sum"),
                order_count=("order_id", "nunique"),
                first_order=("order_date", "min"),
                last_order=("order_date", "max"),
                favorite_category=(
                    "category",
                    lambda s: s.mode().iloc[0] if len(s.mode()) > 0 else "Unknown",
                ),
            )
            .reset_index()
            .sort_values("total_spend", ascending=False)
            .head(top_n)
        )
        agg["total_spend"] = agg["total_spend"].round(2)
        agg["_aggregated_at"] = datetime.now(timezone.utc).isoformat()
        log.info("gold_top_customers", top_n=top_n)
        return agg
