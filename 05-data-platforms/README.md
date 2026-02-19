![MCP](https://img.shields.io/badge/MCP-enabled-brightgreen)
![Agentic AI](https://img.shields.io/badge/Agentic%20AI-supported-blue)
![Bruin](https://img.shields.io/badge/Bruin-CLI-orange)
![Data Platform](https://img.shields.io/badge/Data%20Platform-complete-lightgrey)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)
![VS Code](https://img.shields.io/badge/VS%20Code-007ACC?logo=visual-studio-code&logoColor=white)
![Cursor](https://img.shields.io/badge/Cursor-IDE-lightblue)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-blue)

# 🚕 End-to-End Data Pipeline with Bruin (NYC Taxi Data)

This project demonstrates how to build a complete analytics pipeline using Bruin and DuckDB. The pipeline ingests raw NYC taxi data, transforms it into a clean analytical model, and produces aggregated reporting tables.

The goal was to explore how a single tool can handle ingestion, orchestration, transformations, data quality, lineage, and deployment.

## Why Bruin?

Modern data pipelines often require multiple tools:
- ingestion tools
- transformation frameworks
- orchestration engines
- monitoring and quality tools
- lineage and metadata tooling

In this pipeline, configuration, asset dependencies, and data quality rules are defined together within the project structure.

This project focuses on:
- Pipeline structure and orchestration
- Incremental processing strategies
- Parameterized pipelines
- Data quality checks
- Lineage and dependency management
- Local development with DuckDB

## Project Architecture

The pipeline follows a three-layer architecture:

1. Ingestion layer → raw data extraction
2. Staging layer → cleaning and modeling
3. Reporting layer → analytics and aggregations

DuckDB is used as the local data warehouse.

```text
pipeline/
 ├── pipeline.yml
 └── assets/
     ├── ingestion/
     ├── staging/
     └── reports/
```

## Bruin Project Structure

A Bruin pipeline relies on a few core components:

| File / Folder  | Purpose                                              |
| -------------- | ---------------------------------------------------- |
| `.bruin.yml`   | Local environments and connections (kept out of Git) |
| `pipeline.yml` | Pipeline configuration and variables                 |
| `assets/`      | All ingestion, SQL, and Python assets                |

This separation keeps secrets local while allowing the pipeline definition to remain version controlled.

## Pipeline Configuration

The pipeline is configured to run daily and uses DuckDB as the default connection.

It also defines a custom variable that controls which taxi types are processed:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

Variables can be overridden at runtime to control pipeline behavior. Example: running the pipeline only for yellow taxis.

```bash
bruin run ./pipeline/pipeline.yml --var 'taxi_types=["yellow"]'
```

Using variables like `taxi_types` lets you run the pipeline for different subsets of data without modifying the asset code.

## Ingestion Layer

Python Asset: `ingestion.trips`

This asset downloads monthly NYC taxi parquet files and loads them into DuckDB.

Key design choices:
- Uses `BRUIN_START_DATE` and `BRUIN_END_DATE` to process data by time window
- Reads pipeline variables from `BRUIN_VARS`
- Uses append materialization so new runs only add new data

This layer also includes a **seed asset** that loads a payment type lookup table from CSV.

### Built-in Data Quality Checks

Example checks applied to the lookup table:
- `not_null`
- `unique`

These checks run automatically after the asset finishes.

## Staging Layer

**SQL Asset:** `staging.trips`

This layer cleans and joins raw data and prepares it for analytics.

Important features:
- Joins raw trips with the payment lookup table
- Deduplicates rows using window functions
- Applies column metadata and quality checks

### Incremental materialization strategy

The NYC taxi dataset is naturally partitioned by time using `pickup_datetime`. Because new data arrives monthly and historical months may occasionally need to be reprocessed, the staging layer must support incremental processing by time window.

For this use case, the most appropriate strategy is:

`time_interval`

This strategy:
1. Deletes records in the specified time range
2. Re-inserts the transformed results for that same interval

Why this matters:
- `append` adds new rows but cannot correct or deduplicate existing data
- `replace` rebuilds the full table every run, which is inefficient for large datasets
- `view` does not persist cleaned data and recalculates every query

`time_interval` allows safe reprocessing of a specific month without touching the rest of the dataset.

In this project, the staging asset is configured like this:

```yaml
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp
```

This ensures that only the relevant time window is refreshed on each run while keeping the rest of the table intact.

The reporting asset also uses `time_interval` materialization, refreshing only the relevant date range.

## Reporting Layer

**SQL Asset:** `reports.trips_report`

This asset produces aggregated metrics:
- trips per day
- trips by taxi type
- trips by payment type
- total and average fares

It also runs incrementally using the `time_interval` strategy, refreshing only the relevant date range at the **date level.**

## Running the Pipeline

Validate the project:

```bash
bruin validate ./pipeline/pipeline.yml
```

Run for a test interval:

```bash
bruin run ./pipeline/pipeline.yml \
  --start-date 2022-01-01 \
  --end-date 2022-02-01
```

### First Run / Full Rebuild

When running the pipeline for the first time on a brand new DuckDB database, the tables do not exist yet. In this scenario, the pipeline should rebuild every asset from scratch.

This flag is especially useful when:
- running a pipeline for the first time
- backfilling historical data
- rebuilding after major logic changes

```bash
bruin run ./pipeline/pipeline.yml --full-refresh
```

This forces Bruin to:
1. Drop any existing tables
2. Recreate all assets
3. Reprocess the full historical dataset

## Running an Asset with All Downstream Dependencies

During development, it is common to modify a single asset and then re-run everything that depends on it. For example, if the ingestion logic changes, the staging and reporting layers must be rebuilt so they reflect the updated raw data.

Bruin provides a convenient way to run an asset together with all of its downstream dependencies:

```bash
bruin run ingestion/trips.py --downstream
```

This command will:
- Execute the modified asset
- Automatically run all downstream assets
- Rebuild the pipeline from that point forward

In this project, running the command above triggers:
- `ingestion.trips`
- `staging.trips`
- `reports.trips_report`

This workflow makes iterative development much faster since the entire pipeline does not need to be executed from scratch.

## Data Quality

Example checks used in the pipeline:
- `not_null` on key timestamp columns
- `unique` primary keys
- Custom checks ensuring tables contain rows
- Non-negative metrics in reporting tables

These checks run automatically and fail the pipeline if violated.

Key timestamp fields such as `pickup_datetime` include a `not_null` check to guarantee that critical time information is always present.

## Lineage and Orchestration

Dependencies between assets create an execution graph automatically.

Execution order:
1. Ingestion assets run in parallel
2. Staging runs after ingestion completes
3. Reporting runs after staging completes

Lineage can be visualized using:

```bash
bruin lineage ./assets/reports/trips_report.sql 
```

## Working with Bruin MCP (AI Integration)

Bruin provides an MCP server that connects AI agents to your pipeline. With MCP enabled in VS Code or Cursor, an AI assistant can help explore pipeline logic and data, test asset changes, and assist with query writing. This allows for iterative development and easier testing of pipeline changes.

## Key Takeaways

This project demonstrates how a single platform can manage:
- ingestion
- transformations
- orchestration
- data quality
- lineage
- parameterized runs

All inside one version-controlled repository.

This setup demonstrates how one tool can handle ingestion, transformations, orchestration, data quality, and lineage in a single repository, simplifying development and testing.