"""
OneLake Client — reads and writes Parquet files to Microsoft Fabric OneLake.

OneLake exposes a DFS (Azure Data Lake Storage Gen2) endpoint at:
    https://onelake.dfs.fabric.microsoft.com

Files are organized as:
    <workspace>/<lakehouse>.Lakehouse/Files/<medallion_layer>/<table_name>/data.parquet
"""

from __future__ import annotations

import io
from pathlib import PurePosixPath

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from config.settings import OneLakeConfig
from src.utils.logging import get_logger

log = get_logger(__name__)


class OneLakeClient:
    """Thin wrapper around Azure Data Lake Storage for OneLake."""

    def __init__(self, config: OneLakeConfig) -> None:
        self._cfg = config
        self._credential = DefaultAzureCredential()
        self._service = DataLakeServiceClient(
            account_url=self._cfg.account_url,
            credential=self._credential,
        )
        # The filesystem corresponds to the workspace
        self._fs = self._service.get_file_system_client(self._cfg.workspace_name)

    # ── Helpers ───────────────────────────────────────────────────────

    def _lakehouse_path(
        self, layer: str, table: str, filename: str = "data.parquet"
    ) -> str:
        """Build the full path inside OneLake for a given medallion layer."""
        return str(
            PurePosixPath(
                f"{self._cfg.lakehouse_name}.Lakehouse",
                "Files",
                layer,
                table,
                filename,
            )
        )

    # ── Write ─────────────────────────────────────────────────────────

    def write_parquet(self, df: pd.DataFrame, layer: str, table: str) -> str:
        """
        Write a DataFrame as Parquet into OneLake under the specified
        medallion layer (bronze / silver / gold).

        Returns the OneLake path written to.
        """
        path = self._lakehouse_path(layer, table)
        log.info("onelake_write", path=path, rows=len(df))

        buf = io.BytesIO()
        arrow_table = pa.Table.from_pandas(df)
        pq.write_table(arrow_table, buf)
        buf.seek(0)

        file_client = self._fs.get_file_client(path)
        file_client.upload_data(buf.getvalue(), overwrite=True)

        log.info("onelake_write_done", path=path, size_bytes=buf.tell())
        return path

    # ── Read ──────────────────────────────────────────────────────────

    def read_parquet(self, layer: str, table: str) -> pd.DataFrame:
        """Read a Parquet file from OneLake back into a DataFrame."""
        path = self._lakehouse_path(layer, table)
        log.info("onelake_read", path=path)

        file_client = self._fs.get_file_client(path)
        download = file_client.download_file()
        raw = download.readall()

        buf = io.BytesIO(raw)
        arrow_table = pq.read_table(buf)
        df = arrow_table.to_pandas()
        log.info("onelake_read_done", path=path, rows=len(df))
        return df

    # ── Utilities ─────────────────────────────────────────────────────

    def path_exists(self, layer: str, table: str) -> bool:
        """Check whether a specific table file exists in OneLake."""
        path = self._lakehouse_path(layer, table)
        try:
            file_client = self._fs.get_file_client(path)
            file_client.get_file_properties()
            return True
        except Exception:
            return False

    def list_tables(self, layer: str) -> list[str]:
        """List all table folders under a medallion layer."""
        prefix = str(
            PurePosixPath(f"{self._cfg.lakehouse_name}.Lakehouse", "Files", layer)
        )
        paths = self._fs.get_paths(path=prefix)
        tables = []
        for p in paths:
            if p.is_directory:
                tables.append(PurePosixPath(p.name).name)
        return tables

    def delete_table(self, layer: str, table: str) -> None:
        """Delete a table's parquet file from OneLake."""
        path = self._lakehouse_path(layer, table)
        log.warning("onelake_delete", path=path)
        file_client = self._fs.get_file_client(path)
        file_client.delete_file()
