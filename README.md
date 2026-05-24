# Data Engineering Zoomcamp

Hands-on projects and notes from the Data Engineering Zoomcamp by [Alexey Grigorev](https://github.com/alexeygrigorev) and the [DataTalks.Club](https://github.com/DataTalksClub) team.

This repository follows the full course curriculum and contains exercises, experiments, and the final project.

## Course Overview

The course is a practical, end-to-end introduction to data engineering, including:

- Containerization and infrastructure as code
- Workflow orchestration
- Data ingestion pipelines
- Data warehousing and analytics engineering
- Batch and streaming processing
- A final real-world project with peer review

## Tech Stack

Tools and technologies used throughout the course include:

- Docker and Docker Compose  
- Google Cloud Platform (GCP)  
- Terraform  
- PostgreSQL  
- Kestra  
- BigQuery  
- dbt  
- Apache Spark  
- Kafka  
- Python and SQL  

## Repository Structure

The repository is organized by modules and workshops:

```text
.
├── 01-containerization-iac    # Dockerized Postgres ingestion pipeline, GCP setup with Terraform
├── 02-workflow-orchestration  # Kestra pipelines moving NYC taxi data into GCS and Postgres
├── 03-data-warehousing        # BigQuery external tables, partitioning, clustering
├── 04-analytics-engineering   # dbt models in DuckDB and BigQuery
├── 05-data-platforms          # Bruin end-to-end pipeline, 3-layer medallion architecture
├── 06-agentic-dlt             # dlt pipeline built with MCP and Cursor
├── 07-batch-processing        # PySpark locally and on Dataproc, output to BigQuery
└── 08-streaming               # PyFlink + Redpanda streaming pipeline on green taxi data
```

## Acknowledgements

All course content and structure are provided by:

- Alexey Grigorev  
- The DataTalks.Club team