import pandas as pd
from sqlalchemy import create_engine
import click

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

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    url = f'{prefix}{target_table}_{year}-{month:02d}.parquet'

    # engine = create_engine('postgresql://root:root@pgdatabase:5432/nyc_taxi')
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    try:
        print("Loading green_tripdata...")
        df = pd.read_parquet(url)
        df.to_sql(
            name = target_table,
            con = engine,
            if_exists = 'replace',  # or 'append'
            index = False,
            method = 'multi'
        )
        print("green_tripdata loaded.\n")

        print("Loading taxi_zone_lookup...")
        df_zones = pd.read_csv('/data/taxi_zone_lookup.csv')
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
    run()