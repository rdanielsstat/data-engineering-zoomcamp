## Uncompressed file size for `yellow_tripdata_2020-12.csv`

Added `ls -lh {{render(vars.file)}}` to the commands list in the extract task in the `05_postgres_taxi_scheduled` Kestra workflow code:

```yaml
- id: extract
  type: io.kestra.plugin.scripts.shell.Commands
  outputFiles:
    - "*.csv"
  taskRunner:
    type: io.kestra.plugin.core.runner.Process
  commands:
    - wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{inputs.taxi}}/{{render(vars.file)}}.gz | gunzip > {{render(vars.file)}}
    - ls -lh {{render(vars.file)}}
```

## Rendered value of the `file` variable for green taxi, 2020-04

The variable is defined as:

```yaml
variables:
  file: "{{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy-MM')}}.csv"
```

With the inputs:
- taxi = `green`
- year = `2020`
- month = `04`

`trigger.date` renders to `2020-04`, so the final value becomes:

`green_tripdata_2020-04.csv`

## Total number of rows for Yellow Taxi data in 2020

Loaded all 2020 Yellow Taxi CSV files into PostgreSQL and ran:

```sql
SELECT COUNT(*) 
  FROM public.yellow_tripdata
 WHERE filename LIKE '%2020%' ;
```

The result is:

`24,648,499`

## Total number of rows for Green Taxi data in 2020

Loaded all 2020 Green Taxi CSV files into PostgreSQL and ran:

```sql
SELECT COUNT(*)
  FROM public.green_tripdata
 WHERE filename LIKE '%2020%' ;
```

The result is:

`1,734,051`

## Total number of rows for Yellow Taxi data in March 2021

Filtered the table to the March 2021 file and ran:

```sql
SELECT COUNT(*)
  FROM public.yellow_tripdata
 WHERE filename LIKE '%2021-03%' ;
```

The result is:

`1,925,152`

## Configuring the Schedule trigger timezone to New York

In the Schedule trigger configuration, set the `timezone` property to the IANA timezone name. For example, for the `green_schedule` trigger:

```yaml
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York
    inputs:
        taxi: green
```

This ensures the workflow runs according to New York local time, including daylight saving time adjustments. See screenshot below.

![Schedule trigger timezone configuration](20260127_kestra_trigger_timezone.png)
