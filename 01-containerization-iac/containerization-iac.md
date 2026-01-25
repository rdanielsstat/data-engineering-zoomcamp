# Understanding Docker images

The following starts an interactive container with a bash prompt.

```bash
docker run -it --entrypoint bash python:3.13
```

Output:
```text
Unable to find image 'python:3.13' locally
3.13: Pulling from library/python
4a1c41792403: Pull complete 
c9b629762372: Pull complete 
3fffeb567ed4: Pull complete 
5582010cab7f: Pull complete 
2470fab23101: Pull complete 
599d5b6b6766: Pull complete 
6a2920e3d16b: Pull complete 
Digest: sha256:c8b03b4e98b39cfb180a5ea13ae5ee39039a8f75ccf52fe6d5c216eed6e1be1d
Status: Downloaded newer image for python:3.13
root@e2192db6d18d:/#
```

Once inside the container, the following is run to get the version of pip.

```bash
pip --version
```

Output:

```text
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

# Understanding Docker networking and docker-compose

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

For the above `docker-compose.yaml` file, the `hostname` and `port` that pgadmin should use to connect to the postgres database are `db:5432`.

# SQL

### For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile?

```sql
SELECT COUNT(*) AS short_trips
  FROM green_tripdata
 WHERE lpep_pickup_datetime >= '2025-11-01' AND 
 	   lpep_pickup_datetime < '2025-12-01' AND 
	   trip_distance <= 1 ;
```

### Which was the pick up day with the longest trip distance considering only trips with trip_distance less than 100 miles?

```sql
SELECT DATE(lpep_pickup_datetime) AS pickup_day, MAX(trip_distance) AS max_distance
  FROM green_tripdata
 WHERE trip_distance < 100
 GROUP BY DATE(lpep_pickup_datetime)
 ORDER BY max_distance DESC
 LIMIT 1 ;
```

### Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

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

### For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

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

# Terraform workflow

`terraform init`, `terraform apply -auto-approve`, `terraform destroy` describe:

1. Downloading plugins and setting up backend, 
2. Generating and executing changes, and 
3. Removing all resources

Why:

1. `terraform init`
   - Downloads providers/plugins
   - Sets up backend
   - First step in any Terraform workflow

2. `terraform apply -auto-approve`
   - Generates and executes changes
   - `auto-approve` skips the interactive confirmation

3. `terraform destroy`
   - Removes all resources managed by Terraform