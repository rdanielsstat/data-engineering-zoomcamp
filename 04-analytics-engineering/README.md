![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?logo=googlebigquery&logoColor=white)
![Google Cloud Storage](https://img.shields.io/badge/Google%20Cloud%20Storage-4285F4?logo=googlecloud&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF0000?logo=dbt-labs&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-0052FF?logo=apacheparquet&logoColor=white)
![Data Engineering](https://img.shields.io/badge/Data%20Engineering-00BFFF)
![Analytics Engineering](https://img.shields.io/badge/Analytics%20Engineering-FFA500)

# NYC Taxi Analytics Engineering Project

This project demonstrates analytics engineering using NYC taxi trip data. The goal is to transform, model, and analyze taxi trip datasets using **dbt** with **DuckDB** and **BigQuery**, incorporating testing, documentation, and production deployment practices.  

Key tools and technologies:

- **GCP & BigQuery** – Cloud data warehouse for storing and querying datasets
- **Parquet** – Columnar file format for efficient storage and transfer
- **DuckDB** – Local analytical database for fast testing of transformations
- **dbt Cloud & CLI** – Building, testing, and documenting models
- **Service account key authentication** – Secure access to BigQuery
- **VSCode with dbt Power User** – Development environment for writing and testing dbt models
- **dbt Environments (dev & prod)** – Separate development and production targets allow testing transformations locally before deploying to BigQuery
- **dbt Documentation (schema.yml files)** – Define sources, tests, and column metadata to generate docs and ensure data quality

DuckDB is used for local development and rapid iteration, while BigQuery serves as the production warehouse for analytics.

## Project Structure

The project folder is organized to include both ingestion scripts and the dbt analytics project. The dbt project is housed in `taxi_rides_ny` and contains `schema.yml` files for documentation, testing, and defining sources, along with staging, intermediate, fact models, macros, seeds, and configuration files. Staging models prepare raw data by standardizing columns, filtering nulls, and ensuring consistent data types. Intermediate models join and union multiple staging models to create consolidated datasets. Fact models provide metrics and aggregations used for analytics.

```text
├── DLT_upload_to_DuckDB.ipynb                      # Notebook to ingest data into DuckDB
├── DLT_upload_to_GCP.ipynb                         # Notebook to ingest data into GCP/BigQuery
├── load_fhv_data.py                                # Script to transform FHV CSV files to Parquet
├── load_fhv_data_to_bigquery.py                    # Script to load FHV Parquet files into BigQuery
├── load_taxi_data.py                               # Script to transform Taxi CSV files to Parquet
├── load_taxi_data_to_bigquery.py                   # Script to load Taxi Parquet files into BigQuery
└── taxi_rides_ny/                                  # dbt project
    ├── dbt_project.yml                             # dbt project configuration
    ├── macros/                                     # Custom dbt macros
    ├── models/                                     # dbt models
    │   ├── staging/
    │   │   ├── sources.yml
    │   │   ├── schema.yml
    │   │   ├── stg_green_tripdata.sql
    │   │   ├── stg_yellow_tripdata.sql
    │   │   └── stg_fhv_tripdata.sql
    │   ├── intermediate/
    │   │   ├── schema.yml
    │   │   ├── int_trips.sql
    │   │   └── int_trips_unioned.sql
    │   ├── marts/
    │   │   ├── schema.yml
    │   │   ├── fct_trips.sql
    │   │   ├── dim_zones.sql
    │   │   ├── dim_vendors.sql
    │   │   └── reporting/
    │   │       └── fct_monthly_zone_revenue.sql
    └── seeds/                                      # Seed CSV files loaded by dbt
```

## Data Sources

The main sources loaded into BigQuery include:

- Green taxi trip data (2019–2020)  
- Yellow taxi trip data (2019–2020)  
- FHV (For-Hire Vehicle) trip data (2019)  

These sources are defined in `sources.yml` and linked to the staging models for transformations.

Data were downloaded from the course repositories:

- Taxi trip data: https://github.com/DataTalksClub/nyc-tlc-data/releases
- FHV data: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv

## dbt Model Lineage and Execution

In dbt, models have upstream dependencies that must be built before the dependent models. For example, the intermediate model `int_trips_unioned` depends on both the green and yellow staging models. Running:

```bash
dbt run --select int_trips_unioned
```

Automatically builds all upstream dependencies first:
- `stg_green_tripdata`
- `stg_yellow_tripdata`
- `int_trips_unioned`

This ensures that all required data is processed and available for the unioned model, maintaining a correct lineage and reproducible results.

## dbt Tests and Data Quality

dbt supports generic and custom tests defined in the `schema.yml` files to enforce data quality. For instance, in the `fct_trips` model, we can validate that only accepted values exist for the `payment_type` column:

```yaml
columns:
  - name: payment_type
    tests:
      - accepted_values:
          values: [1, 2, 3, 4, 5]
```

If a new value (for example, `6`) appears in the source data, running:

```bash
dbt test --select fct_trips
```

Will cause dbt to fail the test and return a non-zero exit code. This immediate feedback highlights inconsistencies, helping ensure the reliability of the analytics models.

## Fact Model Record Counts

Counting records in a fact model provides a quick check for completeness and consistency after a build. For instance, querying the `fct_monthly_zone_revenue` model can be done directly in BigQuery:

```sql
-- in BigQuery
SELECT COUNT(*) AS record_count
  FROM `sandbox-486719.dbt_prod.fct_monthly_zone_revenue` ;
```

Or using dbt’s `ref` function to reference the model:

```sql
-- in dbt
SELECT COUNT(*) AS record_count
  FROM {{ ref('fct_monthly_zone_revenue') }} ;
```

Result:

```diff
record_count
-------------
12,184
```

This simple count serves as a sanity check to confirm that the fact model loaded the expected number of records.

## Analyzing Zone Revenue

The `fct_monthly_zone_revenue` model can also be used to analyze revenue performance across pickup zones. For example, to find the zone with the highest total revenue for Green taxis in 2020:

```sql
-- in BigQuery
SELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue
FROM `sandbox-486719.dbt_prod.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1 ;
```

Or using dbt’s `ref` syntax:

```sql
-- in dbt
SELECT pickup_zone, 
       SUM(revenue_monthly_total_amount) AS total_revenue
  FROM {{ ref('fct_monthly_zone_revenue') }}
 WHERE taxi_color = 'green' AND year = 2020
 GROUP BY pickup_zone
 ORDER BY total_revenue DESC
 LIMIT 1 ;
```

Result:

```diff
pickup_zone
-------------
`East Harlem North`
```

This query highlights the highest-grossing area for Green taxi trips, providing insights into where revenue was concentrated during 2020.

## Trip Counts by Month

Aggregating trips by month allows us to track operational metrics and seasonal patterns. For example, to determine the total number of Green taxi trips in October 2019, we can query the monthly fact model:

```sql
-- in BigQuery
-- in BigQuery
SELECT
    SUM(total_monthly_trips) AS october_2019_trips
FROM `sandbox-486719.dbt_prod.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2019
  AND EXTRACT(MONTH FROM revenue_month) = 10 ;
```

Or using dbt’s `ref` function:

```sql
-- in dbt
SELECT SUM(total_monthly_trips) AS october_2019_trips
  FROM {{ ref('fct_monthly_zone_revenue') }}
 WHERE taxi_color = 'green'
   AND year = 2019
   AND month = 10 ;
```

Result:

```diff
october_2019_trips
------------------
384,624
```

This monthly aggregation helps quantify trip volumes and observe trends over time.

## Staging FHV Trip Data

Staging the For-Hire Vehicle (FHV) dataset ensures that the raw data is clean, consistent, and ready for analytics. Key steps include:
- Loading FHV trip data for 2019 into BigQuery
- Filtering out records where `dispatching_base_num` is null
- Renaming fields to match project conventions (e.g., `PUlocationID` to `pickup_location_id`)

The staging SQL model (`stg_fhv_tripdata.sql`) implements these steps:

```sql
  WITH source AS (
SELECT *
  FROM {{ source('raw', 'fhv_tripdata') }}
),

SELECT
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    sr_flag,
    affiliated_base_number
  FROM source
 WHERE dispatching_base_num IS NOT NULL ;
```

Querying the staged table verifies the total number of FHV records:

```sql
-- in BigQuery
SELECT COUNT(*) AS fhv_2019_records
  FROM `sandbox-486719.dbt_prod.stg_fhv_tripdata` ;
```

```sql
-- in dbt
SELECT COUNT(*) AS fhv_2019_records
  FROM {{ ref('stg_fhv_tripdata') }} ;
```

Result:

```diff
fhv_2019_records
----------------
43,244,693
```

This confirms that the staged FHV dataset is complete and ready for further transformations.

## Conclusion

This project demonstrates a complete analytics engineering workflow for NYC taxi data. Key steps include:
- Loading raw Parquet datasets into BigQuery for efficient storage and querying
- Using dbt to build, test, and document transformations
- Organizing data into staging, intermediate, and fact models for clarity and reuse
- Performing analyses to extract operational insights, such as trip volumes, zone revenue, and FHV activity

By combining dbt, BigQuery, and structured data pipelines, this project illustrates how analytics engineers can develop robust, maintainable, and well-documented data models that support reliable decision-making.