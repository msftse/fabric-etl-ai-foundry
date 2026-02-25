"""
CLI entry point for the Fabric ETL + AI Foundry pipeline.

Usage:
    python -m main etl       --source data/sample/orders.csv
    python -m main ask       --source data/sample/orders.csv --question "What are the top categories?"
    python -m main chat      --source data/sample/orders.csv
    python -m main provision
    python -m main suspend
    python -m main full      --source data/sample/orders.csv

    # Confluence commands
    python -m main confluence-seed       # Populate Confluence with sample data
    python -m main confluence-etl        # Extract from Confluence -> OneLake (Bronze/Silver/Gold)

    # Fabric Data Factory pipeline commands
    python -m main deploy-pipeline       # Deploy notebooks + pipeline to Fabric workspace
    python -m main run-pipeline          # Trigger pipeline execution
    python -m main pipeline-status       # Check run status
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from config.settings import AppConfig
from src.orchestrator import PipelineOrchestrator
from src.utils.logging import get_logger

log = get_logger(__name__)


@click.group()
def cli():
    """Fabric ETL + AI Foundry — Medallion Architecture Pipeline."""
    pass


# ── ETL ──────────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--source", required=True, type=click.Path(exists=True), help="Path to source CSV"
)
def etl(source: str):
    """Run the Bronze -> Silver -> Gold ETL pipeline."""
    orch = PipelineOrchestrator()
    gold = orch.run_etl(source)

    click.echo("\n=== Gold Layer Summary ===")
    for name, df in gold.items():
        click.echo(f"  {name}: {len(df)} rows")
        click.echo(f"    Columns: {', '.join(df.columns[:6])}")
    click.echo()


# ── AI Agent ─────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--source", required=True, type=click.Path(exists=True), help="Path to source CSV"
)
@click.option("--question", "-q", required=True, help="Question for the AI analyst")
@click.option("--structured", is_flag=True, help="Return structured DataInsight")
def ask(source: str, question: str, structured: bool):
    """Ask a single question to the AI data analyst."""
    orch = PipelineOrchestrator()
    gold = orch.run_etl(source)

    if structured:
        insight = orch.ask_analyst_structured(question, gold)
        click.echo(f"\n=== Data Insight (confidence: {insight.confidence:.0%}) ===")
        click.echo(f"Summary: {insight.summary}")
        click.echo("\nKey Findings:")
        for f in insight.key_findings:
            click.echo(f"  - {f}")
        click.echo("\nRecommendations:")
        for r in insight.recommendations:
            click.echo(f"  - {r}")
    else:
        answer = orch.ask_analyst(question, gold)
        click.echo(f"\n=== Analyst Response ===\n{answer}")


@cli.command()
@click.option(
    "--source", required=True, type=click.Path(exists=True), help="Path to source CSV"
)
def chat(source: str):
    """Interactive multi-turn chat with the AI analyst."""
    orch = PipelineOrchestrator()
    gold = orch.run_etl(source)

    click.echo("\n=== Interactive Chat with Data Analyst ===")
    click.echo("Type 'quit' to exit.\n")

    questions: list[str] = []
    while True:
        q = click.prompt("You", default="", show_default=False)
        if q.lower() in ("quit", "exit", "q"):
            break
        if not q.strip():
            continue
        questions.append(q)
        responses = orch.chat_with_analyst(questions, gold)
        click.echo(f"\nAnalyst: {responses[-1]}\n")


@cli.command()
@click.option(
    "--source", required=True, type=click.Path(exists=True), help="Path to source CSV"
)
@click.option(
    "--question",
    "-q",
    default="Provide a comprehensive analysis of this sales data.",
    help="Question",
)
def stream(source: str, question: str):
    """Stream the AI analyst's response in real-time."""
    orch = PipelineOrchestrator()
    gold = orch.run_etl(source)
    orch.stream_analyst(question, gold)


# ── Infrastructure ───────────────────────────────────────────────────


@cli.command()
def provision():
    """Provision (or resume) the Fabric capacity."""
    orch = PipelineOrchestrator()
    orch.provision_infrastructure()
    click.echo("Infrastructure ready.")


@cli.command()
def suspend():
    """Suspend the Fabric capacity to stop billing."""
    orch = PipelineOrchestrator()
    orch.teardown_infrastructure()
    click.echo("Capacity suspended — billing stopped.")


# ── Confluence ───────────────────────────────────────────────────────


@cli.command("confluence-seed")
def confluence_seed():
    """Populate Confluence Cloud with sample pages, spaces, and comments."""
    orch = PipelineOrchestrator()
    summary = orch.seed_confluence()

    click.echo("\n=== Confluence Seed Summary ===")
    click.echo(f"  Space: {summary['space']}")
    click.echo(f"  Pages created: {len(summary['pages'])}")
    for title in summary["pages"]:
        click.echo(f"    - {title}")
    click.echo(f"  Comments added: {summary['comments']}")
    click.echo()


@cli.command("confluence-etl")
def confluence_etl():
    """Extract data from Confluence -> OneLake (Bronze/Silver/Gold)."""
    orch = PipelineOrchestrator()
    gold = orch.run_confluence_etl()

    click.echo("\n=== Confluence Gold Layer Summary ===")
    for name, df in gold.items():
        click.echo(f"  {name}: {len(df)} rows")
        if not df.empty:
            click.echo(f"    Columns: {', '.join(df.columns[:6])}")
    click.echo()


# ── Fabric Data Factory Pipeline ─────────────────────────────────


@cli.command("deploy-pipeline")
def deploy_pipeline():
    """Deploy Fabric Notebooks + Data Factory pipeline to the workspace."""
    orch = PipelineOrchestrator()
    result = orch.deploy_fabric_pipeline()

    click.echo("\n=== Fabric Data Factory Deployment ===")
    for role, info in result.items():
        click.echo(f"  {role}: {info['name']} (id: {info['id']})")
    click.echo()


@cli.command("run-pipeline")
@click.option(
    "--pipeline-id",
    default=None,
    help="Pipeline item ID (auto-detected if omitted)",
)
def run_pipeline(pipeline_id: str | None):
    """Trigger an on-demand run of the ConfluenceETL pipeline."""
    orch = PipelineOrchestrator()
    job_id = orch.run_fabric_pipeline(pipeline_id)

    click.echo(f"\n=== Pipeline Run Triggered ===")
    click.echo(f"  Job instance ID: {job_id}")
    click.echo(
        f"  Check status:    python -m main pipeline-status --pipeline-id <id> --job-id {job_id}"
    )
    click.echo()


@cli.command("pipeline-status")
@click.option("--pipeline-id", required=True, help="Pipeline item ID")
@click.option("--job-id", required=True, help="Job instance ID")
def pipeline_status(pipeline_id: str, job_id: str):
    """Check the status of a pipeline run."""
    orch = PipelineOrchestrator()
    status = orch.get_pipeline_status(pipeline_id, job_id)

    click.echo(f"\n=== Pipeline Run Status ===")
    click.echo(f"  Status:       {status.get('status', 'Unknown')}")
    click.echo(f"  Job type:     {status.get('jobType', '')}")
    click.echo(f"  Started:      {status.get('startTimeUtc', '')}")
    click.echo(f"  Ended:        {status.get('endTimeUtc', '')}")
    if status.get("failureReason"):
        click.echo(f"  Failure:      {status['failureReason']}")
    click.echo()


# ── Full E2E ─────────────────────────────────────────────────────────


@cli.command()
@click.option(
    "--source", required=True, type=click.Path(exists=True), help="Path to source CSV"
)
@click.option(
    "--question",
    "-q",
    default="Give me a full analysis with key insights.",
    help="Analysis question",
)
@click.option("--provision/--no-provision", default=False, help="Provision infra first")
def full(source: str, question: str, provision: bool):
    """Run the complete E2E pipeline: infra -> ETL -> AI analysis."""
    orch = PipelineOrchestrator()
    result = orch.run_full_pipeline(source, question=question, provision=provision)

    click.echo("\n=== Gold Layer ===")
    for name, df in result["gold_data"].items():
        click.echo(f"  {name}: {len(df)} rows")

    click.echo(f"\n=== AI Analysis ===\n{result['ai_analysis']}")


if __name__ == "__main__":
    cli()
