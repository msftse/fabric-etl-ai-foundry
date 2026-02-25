"""
Pipeline Orchestrator.

Coordinates the full E2E flow:
  1. (Optional) Provision Fabric capacity
  2. Bronze — ingest raw data into OneLake
  3. Silver — cleanse and transform
  4. Gold   — aggregate into business-ready tables
  5. AI     — launch the AI Foundry analyst agent for Q&A
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd

from config.settings import AppConfig
from src.ai_agent.analyst import DataAnalystAgent
from src.etl.bronze.ingestion import BronzeIngestion
from src.etl.gold.aggregation import GoldAggregation
from src.etl.silver.transform import SilverTransform
from src.infrastructure.fabric_provisioner import FabricProvisioner
from src.onelake.client import OneLakeClient
from src.utils.logging import get_logger

log = get_logger(__name__)


class PipelineOrchestrator:
    """Runs the end-to-end ETL + AI pipeline."""

    def __init__(self, config: AppConfig | None = None) -> None:
        self._cfg = config or AppConfig()
        self._onelake = OneLakeClient(self._cfg.onelake)
        self._bronze = BronzeIngestion(self._onelake)
        self._silver = SilverTransform(self._onelake)
        self._gold = GoldAggregation(self._onelake)

    # ── Infrastructure ───────────────────────────────────────────────

    def provision_infrastructure(self) -> None:
        """Provision (or confirm) the Fabric capacity."""
        provisioner = FabricProvisioner(self._cfg.fabric)

        if provisioner.check_name_available():
            log.info("provisioning_new_capacity")
            provisioner.provision()
        else:
            cap = provisioner.get_capacity()
            log.info("capacity_exists", name=cap.name, state=cap.properties.state)

            if cap.properties.state == "Paused":
                log.info("resuming_paused_capacity")
                provisioner.resume()

    def teardown_infrastructure(self) -> None:
        """Suspend the Fabric capacity to stop billing."""
        provisioner = FabricProvisioner(self._cfg.fabric)
        provisioner.suspend()

    # ── ETL Pipeline ─────────────────────────────────────────────────

    def run_etl(self, source_csv: str | Path) -> dict[str, pd.DataFrame]:
        """
        Execute the full Bronze -> Silver -> Gold ETL pipeline.

        Returns the gold-layer DataFrames.
        """
        log.info("etl_pipeline_start", source=str(source_csv))

        # Bronze: raw ingestion
        bronze_df = self._bronze.ingest_csv(source_csv, table_name="orders")

        # Silver: cleanse and transform
        silver_df = self._silver.transform_orders(bronze_df)

        # Gold: aggregate
        gold_tables = self._gold.build_all(silver_df)

        log.info(
            "etl_pipeline_done",
            bronze_rows=len(bronze_df),
            silver_rows=len(silver_df),
            gold_tables=list(gold_tables.keys()),
        )
        return gold_tables

    # ── AI Agent ─────────────────────────────────────────────────────

    def ask_analyst(self, question: str, gold_data: dict[str, pd.DataFrame]) -> str:
        """Send a one-shot question to the AI analyst agent."""
        agent = DataAnalystAgent(self._cfg.ai, gold_data)
        return asyncio.run(agent.ask(question))

    def ask_analyst_structured(self, question: str, gold_data: dict[str, pd.DataFrame]):
        """Get a structured DataInsight response from the agent."""
        agent = DataAnalystAgent(self._cfg.ai, gold_data)
        return asyncio.run(agent.ask_structured(question))

    def chat_with_analyst(
        self, questions: list[str], gold_data: dict[str, pd.DataFrame]
    ) -> list[str]:
        """Multi-turn conversation with the AI analyst."""
        agent = DataAnalystAgent(self._cfg.ai, gold_data)
        return asyncio.run(agent.chat(questions))

    def stream_analyst(self, question: str, gold_data: dict[str, pd.DataFrame]) -> None:
        """Stream the agent's answer to stdout."""
        agent = DataAnalystAgent(self._cfg.ai, gold_data)
        asyncio.run(agent.stream_answer(question))

    # ── Full E2E ─────────────────────────────────────────────────────

    def run_full_pipeline(
        self,
        source_csv: str | Path,
        question: str = "Give me a full analysis of the sales data with key insights and recommendations.",
        provision: bool = False,
    ) -> dict:
        """
        Run the complete E2E flow: infrastructure -> ETL -> AI analysis.

        Returns a dict with gold_data and ai_analysis.
        """
        log.info("full_pipeline_start")

        # Step 1: Infrastructure (optional)
        if provision:
            self.provision_infrastructure()

        # Step 2-4: ETL
        gold_data = self.run_etl(source_csv)

        # Step 5: AI analysis
        analysis = self.ask_analyst(question, gold_data)

        log.info("full_pipeline_done")
        return {
            "gold_data": gold_data,
            "ai_analysis": analysis,
        }
