![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-2496ED?logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?logo=terraform&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-4285F4?logo=googlecloud&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-CC2927?logo=postgresql&logoColor=white)

# Containerization and Infrastructure Foundations for Data Engineering

This project walks through the foundational tooling used in modern data engineering workflows. The goal is to build a reproducible local environment, explore container networking, analyze real taxi trip data with SQL, and automate cloud infrastructure provisioning using Terraform.

The workflow mirrors how real data platforms are developed: start with isolated environments, move into orchestrated services, analyze data, then provision scalable infrastructure.

## Running Python in a Container

A core principle of data engineering is reproducibility. Containers allow us to package an environment so it behaves the same across machines.

To explore a minimal Python runtime, an interactive container was started from the official Python image:

```bash
docker run -it --entrypoint bash python:3.13
```

Docker pulled the image and launched an interactive shell inside the container.

Once inside, the installed package manager version was inspected:

```bash
pip --version
```

Output:

```text
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

This confirms the Python 3.13 image ships with pip 25.3.

This step demonstrates how containers provide fully packaged runtimes, eliminating dependency drift between environments.

## Container Networking with Docker Compose

Real data systems rarely run as a single service. Databases, orchestration tools, and UIs must communicate across containers. Docker Compose provides a simple way to define multi-service environments.

The following stack launches PostgreSQL and pgAdmin together:

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

Docker Compose automatically creates a shared internal network where services can reach each other using **service names as hostnames**.

Because PostgreSQL exposes port `5432` inside the container and the service name is `db`, pgAdmin connects using:

```makefile
db:5432
```

This highlights an important concept:
Host machine ports and container network ports are different concerns. Containers communicate over the internal Docker network, not the host-mapped ports.

## Preparing the Dataset

To simulate a realistic analytics workflow, NYC Green Taxi data was downloaded:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

The dataset was loaded into PostgreSQL for analysis.

## SQL Analysis of Taxi Trips

Working with SQL is central to analytics engineering and warehousing. The following queries explore trip behavior in November 2025.

### Short Distance Trips

Understanding trip length distribution is a common exploratory task.

```sql
SELECT COUNT(*) AS short_trips
  FROM green_tripdata
 WHERE lpep_pickup_datetime >= '2025-11-01' AND 
 	   lpep_pickup_datetime < '2025-12-01' AND 
	   trip_distance <= 1 ;
```

Result: **8,007 trips**

This query demonstrates filtering by time ranges and applying distance-based constraints.

### Longest trip by pickup day

To identify outliers while avoiding data errors, trips longer than 100 miles were excluded.

```sql
SELECT DATE(lpep_pickup_datetime) AS pickup_day, MAX(trip_distance) AS max_distance
  FROM green_tripdata
 WHERE trip_distance < 100
 GROUP BY DATE(lpep_pickup_datetime)
 ORDER BY max_distance DESC
 LIMIT 1 ;
```

Result: **2025-11-14**

This illustrates grouping, aggregation, and sorting to surface extreme values.

### Highest revenue pickup zone (single day)

Joining fact tables with dimension tables is a core data warehousing pattern.

```sql
SELECT tz."Zone" AS pickup_zone,
       SUM(gtd."total_amount") AS total_amount_sum
  FROM green_tripdata AS gtd
  JOIN taxi_zone_lookup AS tz
       ON gtd."PULocationID" = tz."LocationID"
 WHERE DATE(gtd.lpep_pickup_datetime) = '2025-11-18'
 GROUP BY tz."Zone"
 ORDER BY total_amount_sum DESC
 LIMIT 1 ;
 ```

Result: **East Harlem North**

This demonstrates star-schema style joins and revenue aggregation.

### Largest tip destination from East Harlem North

This query analyzes tipping behavior by combining pickup filtering, time filtering, and zone joins.

```sql
SELECT tz_drop."Zone" AS dropoff_zone, gtd.tip_amount
  FROM green_tripdata AS gtd
  JOIN taxi_zone_lookup AS tz_pick
       ON gtd."PULocationID" = tz_pick."LocationID"
  JOIN taxi_zone_lookup AS tz_drop
       ON gtd."DOLocationID" = tz_drop."LocationID"
 WHERE tz_pick."Zone" = 'East Harlem North' AND 
       gtd.lpep_pickup_datetime >= '2025-11-01' AND 
	   gtd.lpep_pickup_datetime < '2025-12-01'
 ORDER BY gtd.tip_amount DESC
 LIMIT 1 ;
```

Result: **JFK Airport**

This showcases multi-join analytics queries and ranking results.

## Provisioning Cloud Infrastructure with Terraform

Data platforms must be reproducible in the cloud. Infrastructure as Code enables version-controlled environments.

The Terraform workflow used in this project follows the standard lifecycle:

```cpp
terraform init
terraform apply -auto-approve
terraform destroy
```

### What each step represents

**terraform init**
- Downloads provider plugins
- Configures backend state
- Prepares the working directory

**terraform apply -auto-approve**
- Generates the execution plan
- Applies the changes automatically
- Creates cloud resources such as storage buckets and datasets

**terraform destroy**
- Removes all resources managed by Terraform

This workflow ensures infrastructure can be created and removed reliably, enabling repeatable environments for data projects.