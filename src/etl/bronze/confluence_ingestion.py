"""
Bronze Layer — Confluence Data Ingestion.

Reads raw data from Confluence Cloud (spaces, pages, comments) and writes
them as-is into OneLake's bronze zone in Parquet format. Adds ingestion
metadata columns:
  _ingested_at  — UTC timestamp of ingestion
  _source_file  — "confluence_api"
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from src.confluence.client import ConfluenceClient
from src.onelake.client import OneLakeClient
from src.utils.logging import get_logger

log = get_logger(__name__)


class ConfluenceBronzeIngestion:
    """Ingest raw Confluence data into the bronze layer of OneLake."""

    def __init__(self, onelake: OneLakeClient, confluence: ConfluenceClient) -> None:
        self._onelake = onelake
        self._confluence = confluence

    def ingest_all(self) -> dict[str, pd.DataFrame]:
        """
        Extract all data from Confluence and persist to bronze layer.

        Returns a dict of table_name -> raw DataFrame.
        """
        log.info("confluence_bronze_ingest_start")

        # Extract from Confluence
        raw_data = self._confluence.extract_all()

        # Persist each table to bronze
        result = {}
        for table_name, df in raw_data.items():
            if df.empty:
                log.info("confluence_bronze_skip_empty", table=table_name)
                result[table_name] = df
                continue

            df = df.copy()
            df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            df["_source_file"] = "confluence_api"

            self._onelake.write_parquet(df, layer="bronze", table=table_name)
            log.info("confluence_bronze_ingest_done", table=table_name, rows=len(df))
            result[table_name] = df

        log.info(
            "confluence_bronze_ingest_all_done",
            tables=list(result.keys()),
            total_rows=sum(len(df) for df in result.values()),
        )
        return result
