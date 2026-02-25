"""
AI Foundry Data Analyst Agent.

Uses the Microsoft Agent Framework (azure-ai provider) to create a persistent
agent that can:
  1. Answer natural-language questions about the gold-layer data
  2. Run Python code via hosted Code Interpreter to produce charts
  3. Search the web for market context (via Bing)

The agent is given gold-layer summaries as context and uses function tools
to query live OneLake data on demand.
"""

from __future__ import annotations

import asyncio
import json
from typing import Annotated

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from agent_framework import HostedCodeInterpreterTool, HostedWebSearchTool
from agent_framework.azure import AzureAIAgentsProvider
from azure.identity.aio import DefaultAzureCredential

from config.settings import AIFoundryConfig
from src.utils.logging import get_logger

log = get_logger(__name__)

# ────────────────────────────────────────────────────────────────────
# Pydantic model for structured analysis output
# ────────────────────────────────────────────────────────────────────


class DataInsight(BaseModel):
    """Structured analysis result returned by the agent."""

    model_config = ConfigDict(extra="forbid")

    summary: str
    key_findings: list[str]
    recommendations: list[str]
    confidence: float = Field(ge=0.0, le=1.0)


# ────────────────────────────────────────────────────────────────────
# Function tools exposed to the agent
# ────────────────────────────────────────────────────────────────────

# These will be populated by the DataAnalystAgent before the agent is created.
_gold_data_store: dict[str, pd.DataFrame] = {}


def query_revenue_by_country(
    country: Annotated[
        str | None,
        Field(description="ISO country code (e.g. US, UK). Pass None for all."),
    ] = None,
) -> str:
    """Query revenue data aggregated by shipping country."""
    df = _gold_data_store.get("revenue_by_country")
    if df is None:
        return json.dumps({"error": "revenue_by_country data not loaded"})
    if country:
        df = df[df["shipping_country"] == country.upper()]
    return df.to_json(orient="records", date_format="iso")


def query_revenue_by_category(
    category: Annotated[
        str | None,
        Field(description="Product category (e.g. Electronics). Pass None for all."),
    ] = None,
) -> str:
    """Query revenue data aggregated by product category."""
    df = _gold_data_store.get("revenue_by_category")
    if df is None:
        return json.dumps({"error": "revenue_by_category data not loaded"})
    if category:
        df = df[df["category"].str.lower() == category.lower()]
    return df.to_json(orient="records", date_format="iso")


def query_daily_revenue(
    start_date: Annotated[
        str | None, Field(description="Start date YYYY-MM-DD")
    ] = None,
    end_date: Annotated[str | None, Field(description="End date YYYY-MM-DD")] = None,
) -> str:
    """Query daily revenue time series, optionally filtered by date range."""
    df = _gold_data_store.get("daily_revenue")
    if df is None:
        return json.dumps({"error": "daily_revenue data not loaded"})
    df = df.copy()
    df["order_day"] = pd.to_datetime(df["order_day"])
    if start_date:
        df = df[df["order_day"] >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df["order_day"] <= pd.Timestamp(end_date)]
    return df.to_json(orient="records", date_format="iso")


def query_top_customers(
    top_n: Annotated[
        int, Field(description="Number of top customers to return", ge=1, le=50)
    ] = 10,
) -> str:
    """Query top customers by total spend."""
    df = _gold_data_store.get("top_customers")
    if df is None:
        return json.dumps({"error": "top_customers data not loaded"})
    return df.head(top_n).to_json(orient="records", date_format="iso")


def get_data_summary() -> str:
    """Return a high-level summary of all available gold datasets."""
    summary: dict[str, dict] = {}
    for name, df in _gold_data_store.items():
        summary[name] = {
            "rows": len(df),
            "columns": list(df.columns),
            "sample": json.loads(
                df.head(3).to_json(orient="records", date_format="iso")
            ),
        }
    return json.dumps(summary, indent=2)


# ────────────────────────────────────────────────────────────────────
# Agent wrapper
# ────────────────────────────────────────────────────────────────────


SYSTEM_INSTRUCTIONS = """\
You are a senior data analyst specializing in e-commerce and retail analytics.
You have access to gold-layer aggregated data from a Medallion Architecture lakehouse.

Available datasets:
- revenue_by_country: Revenue, order count, avg order value per country
- revenue_by_category: Revenue, units, avg price per product category
- daily_revenue: Day-by-day revenue with cumulative totals
- top_customers: Top customers by total spend with order history

Capabilities:
1. Use the query_* tools to fetch data slices
2. Use the Code Interpreter to run Python for calculations and charts
3. Use Web Search to find market context if relevant
4. Always start by calling get_data_summary() to understand what data is available

Guidelines:
- Be precise with numbers — round to 2 decimal places
- When asked for visualizations, use matplotlib via Code Interpreter
- Provide actionable recommendations alongside analysis
- If data is insufficient, say so rather than guessing
"""


class DataAnalystAgent:
    """Wraps the Azure AI Foundry agent for data analysis."""

    def __init__(
        self, config: AIFoundryConfig, gold_data: dict[str, pd.DataFrame]
    ) -> None:
        self._cfg = config
        self._gold_data = gold_data
        # Populate module-level store so function tools can access data
        _gold_data_store.update(gold_data)

    async def ask(self, question: str) -> str:
        """Send a question to the agent and return the text response."""
        log.info("agent_ask", question=question[:80])

        async with (
            DefaultAzureCredential() as credential,
            AzureAIAgentsProvider(credential=credential) as provider,
        ):
            tools = [
                query_revenue_by_country,
                query_revenue_by_category,
                query_daily_revenue,
                query_top_customers,
                get_data_summary,
                HostedCodeInterpreterTool(),
            ]
            if self._cfg.bing_connection_id:
                tools.append(HostedWebSearchTool(name="Bing"))

            agent = await provider.create_agent(
                name="FabricDataAnalyst",
                instructions=SYSTEM_INSTRUCTIONS,
                tools=tools,
            )

            result = await agent.run(question)
            log.info("agent_response", length=len(result.text))
            return result.text

    async def ask_structured(self, question: str) -> DataInsight:
        """Ask a question and get a structured DataInsight response."""
        log.info("agent_ask_structured", question=question[:80])

        async with (
            DefaultAzureCredential() as credential,
            AzureAIAgentsProvider(credential=credential) as provider,
        ):
            agent = await provider.create_agent(
                name="FabricDataAnalystStructured",
                instructions=SYSTEM_INSTRUCTIONS
                + "\nRespond in the required structured JSON format.",
                tools=[
                    query_revenue_by_country,
                    query_revenue_by_category,
                    query_daily_revenue,
                    query_top_customers,
                    get_data_summary,
                ],
                response_format=DataInsight,
            )

            result = await agent.run(question)
            insight = DataInsight.model_validate_json(result.text)
            log.info("agent_structured_response", confidence=insight.confidence)
            return insight

    async def chat(self, questions: list[str]) -> list[str]:
        """Multi-turn conversation with thread persistence."""
        log.info("agent_chat_start", turns=len(questions))

        async with (
            DefaultAzureCredential() as credential,
            AzureAIAgentsProvider(credential=credential) as provider,
        ):
            tools = [
                query_revenue_by_country,
                query_revenue_by_category,
                query_daily_revenue,
                query_top_customers,
                get_data_summary,
                HostedCodeInterpreterTool(),
            ]

            agent = await provider.create_agent(
                name="FabricDataAnalystChat",
                instructions=SYSTEM_INSTRUCTIONS,
                tools=tools,
            )

            thread = agent.get_new_thread()
            responses: list[str] = []

            for q in questions:
                result = await agent.run(q, thread=thread)
                responses.append(result.text)
                log.info(
                    "agent_chat_turn", question=q[:50], response_len=len(result.text)
                )

            return responses

    async def stream_answer(self, question: str) -> None:
        """Stream the agent's response to stdout."""
        log.info("agent_stream", question=question[:80])

        async with (
            DefaultAzureCredential() as credential,
            AzureAIAgentsProvider(credential=credential) as provider,
        ):
            agent = await provider.create_agent(
                name="FabricDataAnalystStream",
                instructions=SYSTEM_INSTRUCTIONS,
                tools=[
                    query_revenue_by_country,
                    query_revenue_by_category,
                    query_daily_revenue,
                    query_top_customers,
                    get_data_summary,
                    HostedCodeInterpreterTool(),
                ],
            )

            print("\nAnalyst: ", end="", flush=True)
            async for chunk in agent.run_stream(question):
                if chunk.text:
                    print(chunk.text, end="", flush=True)
            print("\n")
