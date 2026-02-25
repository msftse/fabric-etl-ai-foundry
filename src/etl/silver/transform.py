"""
Silver Layer — Data Cleansing and Transformation.

Takes raw bronze data and applies:
  1. Data type casting and standardization
  2. Null handling and deduplication
  3. Column renaming to a consistent schema
  4. Derived columns (e.g. total_amount = quantity * unit_price)
  5. Filtering out invalid / cancelled records
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from src.onelake.client import OneLakeClient
from src.utils.logging import get_logger

log = get_logger(__name__)


class SilverTransform:
    """Cleanse and enrich bronze data, writing results to the silver layer."""

    def __init__(self, onelake: OneLakeClient) -> None:
        self._onelake = onelake

    def transform_orders(self, bronze_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply standard cleansing to the raw orders dataset.

        Steps:
          - Drop ingestion metadata columns
          - Cast dates, numeric types
          - Remove duplicates by order_id
          - Compute total_amount
          - Standardize country codes to uppercase
          - Filter out cancelled orders
          - Add _processed_at timestamp
        """
        log.info("silver_transform_start", input_rows=len(bronze_df))

        df = bronze_df.copy()

        # 1. Drop ingestion metadata (kept only in bronze)
        meta_cols = [c for c in df.columns if c.startswith("_")]
        df = df.drop(columns=meta_cols, errors="ignore")

        # 2. Type casting
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        df["quantity"] = (
            pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        )
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0)

        # 3. Deduplicate
        before = len(df)
        df = df.drop_duplicates(subset=["order_id"], keep="last")
        log.info("silver_dedup", removed=before - len(df))

        # 4. Derived columns
        df["total_amount"] = (df["quantity"] * df["unit_price"]).round(2)

        # 5. Standardize
        df["shipping_country"] = df["shipping_country"].str.upper().str.strip()
        df["category"] = df["category"].str.strip().str.title()
        df["status"] = df["status"].str.strip().str.lower()

        # 6. Remove cancelled orders
        cancelled = len(df[df["status"] == "cancelled"])
        df = df[df["status"] != "cancelled"]
        log.info("silver_filter_cancelled", removed=cancelled)

        # 7. Audit timestamp
        df["_processed_at"] = datetime.now(timezone.utc).isoformat()

        # Persist to silver zone
        self._onelake.write_parquet(df, layer="silver", table="orders")

        log.info("silver_transform_done", output_rows=len(df))
        return df
