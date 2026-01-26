import pandas as pd
from sqlalchemy import create_engine
import click
import pyarrow.parquet as pq
import fsspec

@click.command()
@click.option('--pg-user', default = 'root', help = 'PostgreSQL username')
@click.option('--pg-pass', default = 'root', help = 'PostgreSQL password')
@click.option('--pg-host', default = 'pgdatabase', help = 'PostgreSQL host')
@click.option('--pg-port', default = '5432', help = 'PostgreSQL port')
@click.option('--pg-db', default = 'nyc_taxi', help = 'PostgreSQL database name')
@click.option('--year', default = 2025, type = int, help = 'Data year')
@click.option('--month', default = 11, type = int, help = 'Data month')
@click.option('--chunksize', default = 10000, type = int, help = 'Ingestion chunk size')
@click.option('--target-table', default = 'green_tripdata', help = 'Target table name')

def ingest(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    parquet_url = f'{prefix}{target_table}_{year}-{month:02d}.parquet'
    zones_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    # engine = create_engine('postgresql://root:root@pgdatabase:5432/nyc_taxi')
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    try:
        print(f"Loading {target_table}...")

        # Open the remote parquet file as a file-like object
        with fsspec.open(parquet_url, 'rb') as f:
            parquet_file = pq.ParquetFile(f)

            first_batch = True
            total_rows = 0

            for batch in parquet_file.iter_batches(batch_size = chunksize):
                df_batch = batch.to_pandas()

                df_batch.to_sql(
                    name = target_table,
                    con = engine,
                    if_exists = 'replace' if first_batch else 'append',
                    index = False,
                    method = 'multi'
                )

                rows = len(df_batch)
                total_rows += rows
                print(f"Inserted {rows} rows (total {total_rows})")
                first_batch = False
        
        print(f"{target_table} loaded.\n")

        print("Loading taxi_zone_lookup...")

        df_zones = pd.read_csv(zones_url)

        df_zones.to_sql(
            name = 'taxi_zone_lookup',
            con = engine,
            if_exists = 'replace',
            index = False,
            method = 'multi'
        )

        print("taxi_zone_lookup loaded.")

    except Exception as e:
        print(f"Ingestion failed: {e}")
        raise

if __name__ == '__main__':
    ingest()