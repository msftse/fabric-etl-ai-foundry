# Fabric ETL + AI Foundry — Medallion Architecture E2E Project

End-to-end data platform combining **Azure Fabric** (OneLake Medallion Architecture),
a Python **ETL pipeline**, and an **Azure AI Foundry** agent for automated data analysis.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AZURE FABRIC CAPACITY                        │
│                     (Provisioned via azure-mgmt-fabric)             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐                   │
│  │  BRONZE   │────▶│  SILVER   │────▶│   GOLD   │                   │
│  │  (Raw)    │     │ (Cleansed)│     │(Aggregated)│                  │
│  └──────────┘     └──────────┘     └─────┬────┘                   │
│       │                                    │                        │
│  OneLake DFS   ◀──── Parquet Files ────▶  │                        │
│                                            │                        │
├────────────────────────────────────────────┼────────────────────────┤
│                                            │                        │
│  ┌─────────────────────────────────────────▼──────────────────┐    │
│  │              AZURE AI FOUNDRY AGENT                         │    │
│  │  ┌───────────────┐  ┌──────────────┐  ┌───────────────┐   │    │
│  │  │ Function Tools │  │Code Interpret│  │  Bing Search  │   │    │
│  │  │ (query data)   │  │ (charts/calc)│  │  (context)    │   │    │
│  │  └───────────────┘  └──────────────┘  └───────────────┘   │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
fabric-etl-ai-foundry/
├── main.py                          # CLI entry point
├── requirements.txt
├── .env.example
├── config/
│   └── settings.py                  # Typed configuration from env vars
├── data/
│   └── sample/
│       └── orders.csv               # Sample e-commerce order data
└── src/
    ├── infrastructure/
    │   └── fabric_provisioner.py    # Fabric capacity CRUD (azure-mgmt-fabric)
    ├── onelake/
    │   └── client.py                # OneLake DFS read/write (Parquet)
    ├── etl/
    │   ├── bronze/
    │   │   └── ingestion.py         # Raw CSV → Parquet ingestion
    │   ├── silver/
    │   │   └── transform.py         # Cleansing, dedup, type casting
    │   └── gold/
    │       └── aggregation.py       # Business aggregations
    ├── ai_agent/
    │   └── analyst.py               # AI Foundry data analyst agent
    ├── orchestrator.py              # E2E pipeline coordinator
    └── utils/
        └── logging.py               # Structured logging
```

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your Azure subscription, resource group, and AI project endpoint
```

### 3. Run the ETL pipeline only

```bash
python main.py etl --source data/sample/orders.csv
```

### 4. Ask the AI analyst a question

```bash
python main.py ask --source data/sample/orders.csv \
  -q "Which country generates the most revenue and why?"
```

### 5. Get structured analysis

```bash
python main.py ask --source data/sample/orders.csv \
  -q "Summarize the sales performance" --structured
```

### 6. Interactive chat

```bash
python main.py chat --source data/sample/orders.csv
```

### 7. Full E2E pipeline (infra + ETL + AI)

```bash
python main.py full --source data/sample/orders.csv --provision
```

### 8. Manage infrastructure

```bash
python main.py provision   # Create / resume Fabric capacity
python main.py suspend     # Pause capacity (stops billing)
```

## Medallion Layers

| Layer | Purpose | Location in OneLake |
|-------|---------|---------------------|
| **Bronze** | Raw ingestion with metadata (`_ingested_at`, `_source_file`) | `Files/bronze/orders/data.parquet` |
| **Silver** | Cleansed: type casting, dedup, derived `total_amount`, filter cancelled | `Files/silver/orders/data.parquet` |
| **Gold** | Aggregated: `revenue_by_country`, `revenue_by_category`, `daily_revenue`, `top_customers` | `Files/gold/<table>/data.parquet` |

## AI Agent Capabilities

The AI Foundry agent (`DataAnalystAgent`) is built with the Microsoft Agent Framework and has:

- **5 function tools** that query gold-layer data (revenue by country/category, daily revenue, top customers, data summary)
- **Code Interpreter** for running Python calculations and generating matplotlib charts
- **Bing Web Search** (optional) for market context
- **Structured output** mode returning `DataInsight` Pydantic models
- **Streaming** for real-time responses
- **Multi-turn threads** for conversational analysis

## Key Technologies

| Component | Technology |
|-----------|-----------|
| Infrastructure | `azure-mgmt-fabric` — Fabric capacity provisioning |
| Storage | OneLake via `azure-storage-file-datalake` (DFS endpoint) |
| ETL | `pandas` + `pyarrow` for Parquet read/write |
| AI Agent | `agent-framework[azure]` — Microsoft Agent Framework |
| Auth | `azure-identity` — DefaultAzureCredential |
| CLI | `click` |
| Logging | `structlog` |

## Required Azure Resources

1. **Azure Subscription** with Fabric capacity enabled
2. **Resource Group** for the Fabric capacity
3. **Azure AI Foundry Project** with a deployed model (e.g. `gpt-4o-mini`)
4. **(Optional)** Bing Search connection for web search tool
