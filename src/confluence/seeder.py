"""
Confluence Data Seeder.

Populates a Confluence Cloud instance with sample data for the ETL demo:
  - Creates a sample space
  - Creates pages with realistic content (project docs, meeting notes, etc.)
  - Adds comments to pages
"""

from __future__ import annotations

import time

from atlassian import Confluence

from config.settings import ConfluenceConfig
from src.utils.logging import get_logger

log = get_logger(__name__)

# ── Sample Data ──────────────────────────────────────────────────────

SPACE_KEY = "ETLDEMO"
SPACE_NAME = "ETL Demo Project"
SPACE_DESCRIPTION = "Sample Confluence space for the Fabric ETL pipeline demo."

SAMPLE_PAGES = [
    {
        "title": "Project Overview",
        "body": """
<h2>ETL Pipeline Project Overview</h2>
<p>This project implements a modern data pipeline using Microsoft Fabric and Azure AI Foundry.</p>

<h3>Architecture</h3>
<p>The pipeline follows the <strong>Medallion Architecture</strong> pattern:</p>
<ul>
  <li><strong>Bronze Layer</strong> — Raw data ingestion from multiple sources</li>
  <li><strong>Silver Layer</strong> — Data cleansing, deduplication, and standardization</li>
  <li><strong>Gold Layer</strong> — Business-ready aggregations and KPIs</li>
</ul>

<h3>Data Sources</h3>
<table>
  <tr><th>Source</th><th>Type</th><th>Frequency</th></tr>
  <tr><td>E-commerce Orders</td><td>CSV</td><td>Daily</td></tr>
  <tr><td>Confluence Knowledge Base</td><td>REST API</td><td>Weekly</td></tr>
  <tr><td>Snowflake Warehouse</td><td>JDBC</td><td>Hourly</td></tr>
</table>

<h3>Team</h3>
<p>Project lead: Roey Zalta<br/>
Data Engineer: Alex Chen<br/>
ML Engineer: Sarah Johnson</p>
""",
        "comments": [
            "Great overview! Should we add the AI agent architecture diagram here?",
            "Agreed, I'll update this page with the system design diagram next sprint.",
        ],
    },
    {
        "title": "Sprint 1 - Meeting Notes",
        "body": """
<h2>Sprint 1 Kickoff Meeting</h2>
<p><strong>Date:</strong> 2025-01-15<br/>
<strong>Attendees:</strong> Roey, Alex, Sarah, David</p>

<h3>Agenda</h3>
<ol>
  <li>Project scope review</li>
  <li>Sprint 1 goals and deliverables</li>
  <li>Technical architecture decisions</li>
  <li>Risk assessment</li>
</ol>

<h3>Decisions</h3>
<ul>
  <li>Use <strong>Microsoft Fabric F2 SKU</strong> for development environment</li>
  <li>Adopt <strong>Medallion Architecture</strong> (Bronze/Silver/Gold) for data layers</li>
  <li>Store all processed data in <strong>OneLake</strong> as Parquet files</li>
  <li>Use <strong>Azure AI Foundry</strong> for the AI analyst agent</li>
</ul>

<h3>Action Items</h3>
<table>
  <tr><th>Task</th><th>Owner</th><th>Due Date</th></tr>
  <tr><td>Set up Fabric capacity</td><td>Roey</td><td>2025-01-20</td></tr>
  <tr><td>Implement bronze ingestion</td><td>Alex</td><td>2025-01-25</td></tr>
  <tr><td>Design AI agent tools</td><td>Sarah</td><td>2025-01-22</td></tr>
  <tr><td>Create sample dataset</td><td>David</td><td>2025-01-18</td></tr>
</table>

<h3>Risks</h3>
<p>Main risk: OneLake DFS endpoint latency may affect pipeline throughput. Mitigation: batch writes with larger Parquet files.</p>
""",
        "comments": [
            "I've completed the Fabric capacity provisioning. F2 is active in West US 3.",
            "Bronze ingestion is working for CSV sources. JSON support coming next.",
            "AI agent prototype is ready with Code Interpreter and web search tools.",
        ],
    },
    {
        "title": "Data Dictionary - Orders",
        "body": """
<h2>E-Commerce Orders Data Dictionary</h2>
<p>This document describes the schema for the orders dataset used in the ETL pipeline.</p>

<h3>Bronze Layer (Raw)</h3>
<table>
  <tr><th>Column</th><th>Type</th><th>Description</th></tr>
  <tr><td>order_id</td><td>string</td><td>Unique order identifier</td></tr>
  <tr><td>customer_id</td><td>string</td><td>Customer identifier</td></tr>
  <tr><td>order_date</td><td>string</td><td>Date the order was placed (ISO format)</td></tr>
  <tr><td>product_name</td><td>string</td><td>Name of the product ordered</td></tr>
  <tr><td>category</td><td>string</td><td>Product category (Electronics, Furniture)</td></tr>
  <tr><td>quantity</td><td>integer</td><td>Number of units ordered</td></tr>
  <tr><td>unit_price</td><td>float</td><td>Price per unit in USD</td></tr>
  <tr><td>status</td><td>string</td><td>Order status (delivered, shipped, pending, cancelled)</td></tr>
  <tr><td>shipping_country</td><td>string</td><td>Destination country</td></tr>
</table>

<h3>Silver Layer (Cleaned)</h3>
<p>Additional derived columns after transformation:</p>
<table>
  <tr><th>Column</th><th>Type</th><th>Description</th></tr>
  <tr><td>total_amount</td><td>float</td><td>quantity x unit_price (computed)</td></tr>
  <tr><td>_processed_at</td><td>datetime</td><td>UTC timestamp of silver processing</td></tr>
</table>
<p><em>Note: Cancelled orders are filtered out in the silver layer.</em></p>

<h3>Gold Layer (Aggregated)</h3>
<p>Four gold tables are produced:</p>
<ul>
  <li><strong>revenue_by_country</strong> — total revenue, order count, avg order value per country</li>
  <li><strong>revenue_by_category</strong> — total revenue, units, avg price per category</li>
  <li><strong>daily_revenue</strong> — revenue by day with cumulative totals</li>
  <li><strong>top_customers</strong> — top 10 customers by total spend</li>
</ul>
""",
        "comments": [
            "Can we add a column for payment method in the next iteration?",
            "Yes, we'll add payment_method and shipping_cost in Sprint 2.",
        ],
    },
    {
        "title": "Runbook - Pipeline Operations",
        "body": """
<h2>Pipeline Operations Runbook</h2>
<p>This document covers day-to-day operations of the ETL pipeline.</p>

<h3>Starting the Pipeline</h3>
<ac:structured-macro ac:name="code">
<ac:plain-text-body><![CDATA[
# Run the full ETL pipeline
python -m main etl --source data/sample/orders.csv

# Run with infrastructure provisioning
python -m main full --source data/sample/orders.csv --provision

# Ask the AI analyst a question
python -m main ask --source data/sample/orders.csv -q "What are the top revenue countries?"
]]></ac:plain-text-body>
</ac:structured-macro>

<h3>Monitoring</h3>
<p>Pipeline logs use <code>structlog</code> with ISO timestamps. Key events to monitor:</p>
<ul>
  <li><code>etl_pipeline_start</code> — pipeline initiated</li>
  <li><code>bronze_ingest_done</code> — raw data loaded</li>
  <li><code>silver_transform_done</code> — data cleansed</li>
  <li><code>gold_build_done</code> — aggregations complete</li>
</ul>

<h3>Troubleshooting</h3>
<table>
  <tr><th>Issue</th><th>Cause</th><th>Resolution</th></tr>
  <tr><td>401 on OneLake write</td><td>Expired credentials</td><td>Re-authenticate with <code>az login</code></td></tr>
  <tr><td>Capacity unavailable</td><td>Fabric capacity paused</td><td>Run <code>python -m main provision</code></td></tr>
  <tr><td>Empty gold tables</td><td>All orders cancelled</td><td>Check source data for valid orders</td></tr>
</table>

<h3>Scaling</h3>
<p>To scale the Fabric capacity from F2 to F4:</p>
<ac:structured-macro ac:name="code">
<ac:plain-text-body><![CDATA[
from src.infrastructure.fabric_provisioner import FabricProvisioner
provisioner = FabricProvisioner(config.fabric)
provisioner.scale("F4")
]]></ac:plain-text-body>
</ac:structured-macro>
""",
        "comments": [
            "We should add alerting for pipeline failures. Can we integrate with Azure Monitor?",
            "Good idea. I'll create a follow-up ticket for Azure Monitor integration.",
            "Also, we need to document the backup and recovery process for OneLake data.",
        ],
    },
    {
        "title": "Architecture Decision Record - AI Agent",
        "body": """
<h2>ADR-001: AI Data Analyst Agent Architecture</h2>

<h3>Status</h3>
<p><strong>Accepted</strong> — 2025-01-20</p>

<h3>Context</h3>
<p>We need an AI-powered interface for business users to query the gold layer data
without writing SQL or Python code. The agent should be able to:</p>
<ul>
  <li>Answer natural language questions about sales data</li>
  <li>Generate visualizations and perform calculations</li>
  <li>Search the web for market context</li>
  <li>Return structured insights with confidence scores</li>
</ul>

<h3>Decision</h3>
<p>We will use the <strong>Microsoft Agent Framework</strong> with <strong>Azure AI Foundry</strong> as the backend.
The agent will be equipped with these tools:</p>
<ol>
  <li><strong>Function Tools</strong> — Custom Python functions to query gold DataFrames</li>
  <li><strong>Code Interpreter</strong> — For ad-hoc analysis and chart generation</li>
  <li><strong>Bing Web Search</strong> — For supplementing analysis with market data</li>
</ol>

<h3>Consequences</h3>
<ul>
  <li><strong>Positive:</strong> Business users can self-serve analytics without technical skills</li>
  <li><strong>Positive:</strong> Structured output (Pydantic models) ensures consistent response format</li>
  <li><strong>Negative:</strong> Azure AI Foundry dependency increases cloud costs</li>
  <li><strong>Negative:</strong> Agent responses may vary — need guardrails for accuracy</li>
</ul>

<h3>Alternatives Considered</h3>
<table>
  <tr><th>Option</th><th>Pros</th><th>Cons</th></tr>
  <tr><td>Direct OpenAI API</td><td>Simpler setup</td><td>No built-in tools, more code needed</td></tr>
  <tr><td>LangChain</td><td>Rich ecosystem</td><td>Heavier dependency, complex abstractions</td></tr>
  <tr><td>Semantic Kernel</td><td>MS ecosystem</td><td>Less mature Python support at time of evaluation</td></tr>
</table>
""",
        "comments": [
            "Solid ADR. The Agent Framework choice aligns well with our Azure-first strategy.",
            "Should we consider adding a Fabric Data Agent connection for direct OneLake queries?",
            "Yes, that's planned for Sprint 3. The connection ID is already in the config.",
        ],
    },
    {
        "title": "Sprint 2 - Retrospective",
        "body": """
<h2>Sprint 2 Retrospective</h2>
<p><strong>Date:</strong> 2025-02-10<br/>
<strong>Sprint Duration:</strong> 2 weeks (Jan 27 - Feb 7)</p>

<h3>What Went Well</h3>
<ul>
  <li>Gold layer aggregations are producing accurate results</li>
  <li>AI agent successfully answers complex multi-step questions</li>
  <li>OneLake write performance improved after batching optimization</li>
  <li>Team velocity increased by 20% compared to Sprint 1</li>
</ul>

<h3>What Could Be Improved</h3>
<ul>
  <li>Silver layer deduplication logic needs more edge case handling</li>
  <li>No automated tests yet - technical debt accumulating</li>
  <li>Documentation is scattered across different tools</li>
  <li>Need better error handling for API rate limits</li>
</ul>

<h3>Action Items for Sprint 3</h3>
<table>
  <tr><th>Action</th><th>Owner</th><th>Priority</th></tr>
  <tr><td>Add unit tests for ETL layers</td><td>Alex</td><td>High</td></tr>
  <tr><td>Implement Confluence ETL connector</td><td>Roey</td><td>High</td></tr>
  <tr><td>Add retry logic for OneLake writes</td><td>Alex</td><td>Medium</td></tr>
  <tr><td>Set up CI/CD pipeline</td><td>David</td><td>Medium</td></tr>
  <tr><td>Explore Fabric Data Agent integration</td><td>Sarah</td><td>Low</td></tr>
</table>

<h3>Metrics</h3>
<table>
  <tr><th>Metric</th><th>Sprint 1</th><th>Sprint 2</th><th>Change</th></tr>
  <tr><td>Stories Completed</td><td>8</td><td>12</td><td>+50%</td></tr>
  <tr><td>Story Points</td><td>21</td><td>34</td><td>+62%</td></tr>
  <tr><td>Bugs Found</td><td>5</td><td>3</td><td>-40%</td></tr>
  <tr><td>Pipeline Runtime (sec)</td><td>45</td><td>28</td><td>-38%</td></tr>
</table>
""",
        "comments": [
            "Good progress! Let's prioritize the test coverage in Sprint 3.",
            "The Confluence ETL connector will help consolidate our documentation data.",
        ],
    },
]


class ConfluenceSeeder:
    """Populate a Confluence Cloud instance with sample data for the ETL demo."""

    def __init__(self, config: ConfluenceConfig) -> None:
        if not config.is_configured:
            raise ValueError(
                "Confluence is not configured. Set CONFLUENCE_URL, "
                "CONFLUENCE_EMAIL, and CONFLUENCE_API_TOKEN in your .env file."
            )
        self._api = Confluence(
            url=config.url,
            username=config.email,
            password=config.api_token,
            cloud=True,
        )
        log.info("confluence_seeder_init", url=config.url)

    def seed(self) -> dict:
        """
        Create sample space, pages, and comments in Confluence.
        Returns a summary of what was created.
        """
        summary = {"space": None, "pages": [], "comments": 0}

        # 1. Create space (or skip if it exists)
        space = self._ensure_space()
        summary["space"] = SPACE_KEY

        # 2. Create pages
        for page_data in SAMPLE_PAGES:
            page_id = self._create_page(
                space_key=SPACE_KEY,
                title=page_data["title"],
                body=page_data["body"],
            )
            if page_id:
                summary["pages"].append(page_data["title"])

                # 3. Add comments
                for comment_text in page_data.get("comments", []):
                    self._add_comment(page_id, comment_text)
                    summary["comments"] += 1
                    time.sleep(0.3)  # Respect API rate limits

            time.sleep(0.5)  # Respect API rate limits

        log.info(
            "confluence_seed_done",
            space=summary["space"],
            pages=len(summary["pages"]),
            comments=summary["comments"],
        )
        return summary

    def _ensure_space(self) -> dict:
        """Create the demo space if it doesn't already exist."""
        try:
            existing = self._api.get_space(SPACE_KEY, expand="description.plain")
            if existing and existing.get("key") == SPACE_KEY:
                log.info("confluence_space_exists", key=SPACE_KEY)
                return existing
        except Exception:
            pass

        log.info("confluence_create_space", key=SPACE_KEY, name=SPACE_NAME)
        space = self._api.create_space(SPACE_KEY, SPACE_NAME)
        return space

    def _create_page(self, space_key: str, title: str, body: str) -> str | None:
        """Create a page in the given space. Returns the page ID or None."""
        try:
            # Check if page already exists
            existing = self._api.get_page_by_title(space_key, title)
            if existing:
                log.info("confluence_page_exists", title=title)
                return existing.get("id")
        except Exception:
            pass

        try:
            log.info("confluence_create_page", title=title)
            result = self._api.create_page(
                space=space_key,
                title=title,
                body=body,
                representation="storage",
            )
            return result.get("id")
        except Exception as exc:
            log.error("confluence_create_page_failed", title=title, error=str(exc))
            return None

    def _add_comment(self, page_id: str, comment_body: str) -> None:
        """Add a comment to a page."""
        try:
            html_body = f"<p>{comment_body}</p>"

            # Use the REST API directly to add a page comment
            self._api.post(
                f"rest/api/content",
                data={
                    "type": "comment",
                    "container": {"id": page_id, "type": "page"},
                    "body": {
                        "storage": {
                            "value": html_body,
                            "representation": "storage",
                        }
                    },
                },
            )
            log.info("confluence_add_comment", page_id=page_id)
        except Exception as exc:
            log.error(
                "confluence_add_comment_failed",
                page_id=page_id,
                error=str(exc),
            )
