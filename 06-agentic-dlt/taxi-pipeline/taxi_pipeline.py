import dlt
import requests

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

@dlt.resource(name="trips", write_disposition="replace")
def get_trips():
    page = 1
    while True:
        response = requests.get(BASE_URL, params={"page": page, "per_page": 1000})
        data = response.json()
        if not data:
            break
        yield data
        page += 1

pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    progress="log",
)

if __name__ == "__main__":
    load_info = pipeline.run(get_trips())
    print(load_info)