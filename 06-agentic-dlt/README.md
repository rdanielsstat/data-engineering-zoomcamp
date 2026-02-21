![dlt](https://img.shields.io/badge/dlt-modern%20data%20ingestion-4B32C3)
![Agentic AI](https://img.shields.io/badge/Agentic%20AI-supported-blue)
![MCP](https://img.shields.io/badge/MCP-enabled-brightgreen)
![Data Engineering](https://img.shields.io/badge/Data%20Engineering-pipeline-blueviolet)
![ELT](https://img.shields.io/badge/ELT-workflow-orange)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)
![VS Code](https://img.shields.io/badge/VS%20Code-007ACC?logo=visual-studio-code&logoColor=white)
![Cursor](https://img.shields.io/badge/Cursor-IDE-lightblue)

# From API to Warehouse

#### AI-Assisted Data Ingestion with dlt, MCP, and Agentic IDEs

## Overview

This project demonstrates a complete modern data engineering workflow using:
- dlt (data load tool) for ingestion and normalization
- DuckDB as a local analytical warehouse
- MCP (Model Context Protocol) for AI-assisted pipeline inspection
- Agentic IDEs (Cursor & VS Code + Copilot) for natural language-driven development
- uv for fast Python dependency management
- dlt Dashboard for metadata and pipeline introspection

The goal was simple:

Go from raw API data to a fully queryable analytics dataset, then explore it using AI, SQL, and visualization tools. Instead of writing everything manually, I leveraged AI agents to scaffold, debug, and inspect the pipeline while I focused on understanding the architecture and results.

## Architecture

#### Extract → Normalize → Load → Inspect → Analyze

1. Extract data via a dlt source
2. Normalize nested data into relational tables
3. Load into DuckDB
3. Inspect via:
   - dlt pipeline taxi_pipeline show
   - MCP server (chat-based inspection)
5. Analyze with SQL and Python

## Tech Stack

- Python 3.11+
- `dlt[duckdb]`
- DuckDB
- uv
- Cursor (agentic IDE)
- VS Code + Copilot
- MCP server (dlt_mcp)
- dlt Dashboard
- marimo (exploration layer)

## Building the Pipeline with AI

After initializing the workspace:

```bash
dlt init dlthub:open_library duckdb
```

I configured the MCP server inside both Cursor and VS Code to give the agent contextual awareness of:
- dlt documentation
- Pipeline metadata
- Schema definitions
- Loaded tables

This allowed me to ask questions like:
- "What tables were created?"
- "What columns exist in the trips table?"
- "How many rows were loaded?"

The agent could answer directly from pipeline state.

## Inspecting the Pipeline

Once the pipeline ran successfully, I launched the dashboard:

```bash
uv run dlt pipeline taxi_pipeline show
```

The dashboard provided:
- Run history
- Schema inspection
- Table-level metadata
- Direct query interface

This incredibly powerful for debugging and validation.

## Analytical Exploration

To validate the dataset and demonstrate querying capability, I connected directly to DuckDB:

```python
import duckdb

con = duckdb.connect("taxi_pipeline.duckdb")
```

### Dataset Time Range

```sql
SELECT MIN(trip_pickup_date_time) AS start_date,
       MAX(trip_dropoff_date_time) AS end_date
  FROM taxi_pipeline_dataset.trips
```

**Result**: 2009-06-01 to 2009-07-01

This confirms the ingestion window and validates that the pipeline extracted the correct slice of data.

### Payment Method Distribution

```sql
SELECT payment_type,
       COUNT(*) AS trip_count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
  FROM taxi_pipeline_dataset.trips
 GROUP BY payment_type
 ORDER BY trip_count DESC
```

Credit card payments account for: 26.66% of total trips

This demonstrates:
- Window functions in DuckDB
- Aggregation over normalized tables
- Clean analytical modeling through dlt

### Total Tips Generated

```sql
SELECT SUM(tip_amt) AS total_tips
  FROM taxi_pipeline_dataset.trips
```

Total tips collected: $6,063.41

This confirms:
- Monetary aggregation works correctly
- Numeric types were properly inferred and loaded
- The dataset is analytics-ready

## What This Project Demonstrates

#### AI-augmented data engineering

Instead of manually writing every function:

- The agent scaffolded sources
- Helped debug errors
- Answered schema questions
- Assisted in writing SQL

The result is faster iteration without sacrificing understanding.

#### MCP as a force multiplier

The Model Context Protocol (MCP) enabled:
- Direct access to pipeline metadata
- Context-aware responses from the agent
- Conversational debugging

This bridges documentation, runtime metadata, and developer experience in a way traditional tooling does not.

#### dlt as a modern ingestion framework

dlt handles:
- Schema inference
- Nested normalization
- Incremental loading
- State management
- Destination abstraction

Instead of hand-writing ETL scripts, you focus on modeling and analytics.

#### DuckDB as a local analytics engine

DuckDB makes it easy to:
- Run analytical SQL locally
- Inspect intermediate results
- Avoid spinning up external infrastructure

Perfect for development and experimentation.

## Development Workflow

- uv for fast dependency management
- Cursor for end-to-end agentic workflow
- VS Code + Copilot for alternate AI-assisted iteration
- dlt Dashboard for runtime introspection
- Direct SQL validation in DuckDB

This setup mirrors a modern, production-style workflow while remaining lightweight and reproducible.

## Key Takeaways
- AI can accelerate data engineering without abstracting away understanding.
- MCP-enabled agents can reason about real pipeline state.
- dlt dramatically reduces boilerplate in ingestion pipelines.
- DuckDB is an excellent local warehouse for rapid iteration.
- Observability via the dlt dashboard makes debugging intuitive.

## Reproducibility

Clone the repo, then:

```bash
pip install "dlt[workspace]"
uv sync
python taxi_pipeline.py
uv run dlt pipeline taxi_pipeline show
```

Then query the generated taxi_pipeline.duckdb database.