"""
Bronze Layer — Raw Data Ingestion.

Reads raw CSV/JSON source files and writes them as-is into OneLake's
bronze zone in Parquet format. Adds ingestion metadata columns:
  _ingested_at  — UTC timestamp of ingestion
  _source_file  — original filename
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.onelake.client import OneLakeClient
from src.utils.logging import get_logger

log = get_logger(__name__)


class BronzeIngestion:
    """Ingest raw source files into the bronze layer of OneLake."""

    def __init__(self, onelake: OneLakeClient) -> None:
        self._onelake = onelake

    def ingest_csv(self, file_path: str | Path, table_name: str) -> pd.DataFrame:
        """
        Read a CSV file, stamp it with ingestion metadata, and persist to
        the bronze layer in OneLake.

        Returns the raw DataFrame for downstream use.
        """
        file_path = Path(file_path)
        log.info("bronze_ingest_start", source=file_path.name, table=table_name)

        df = pd.read_csv(file_path)

        # Add metadata columns
        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source_file"] = file_path.name

        # Persist to OneLake bronze zone
        self._onelake.write_parquet(df, layer="bronze", table=table_name)

        log.info("bronze_ingest_done", rows=len(df), table=table_name)
        return df

    def ingest_json(self, file_path: str | Path, table_name: str) -> pd.DataFrame:
        """Same as ingest_csv but for JSON files."""
        file_path = Path(file_path)
        log.info("bronze_ingest_start", source=file_path.name, table=table_name)

        df = pd.read_json(file_path)
        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source_file"] = file_path.name

        self._onelake.write_parquet(df, layer="bronze", table=table_name)

        log.info("bronze_ingest_done", rows=len(df), table=table_name)
        return df

    def ingest_dataframe(
        self, df: pd.DataFrame, table_name: str, source_label: str = "api"
    ) -> pd.DataFrame:
        """Ingest an in-memory DataFrame (e.g. from an API) into bronze."""
        log.info("bronze_ingest_start", source=source_label, table=table_name)

        df = df.copy()
        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source_file"] = source_label

        self._onelake.write_parquet(df, layer="bronze", table=table_name)

        log.info("bronze_ingest_done", rows=len(df), table=table_name)
        return df
