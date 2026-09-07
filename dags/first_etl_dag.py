from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="first_etl_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["learning", "etl"],
)
def first_etl_pipeline():

    @task
    def extract():
        network_data = {
            "device": "Router-01",
            "latency_ms": 180,
            "packet_loss_percent": 4,
            "status": "degraded",
        }

        print("Extracted data:")
        print(network_data)

        return network_data

    @task
    def transform(network_data):

        latency = network_data["latency_ms"]
        packet_loss = network_data["packet_loss_percent"]

        severity_score = latency + (packet_loss * 20)

        network_data["severity_score"] = severity_score

        print("Transformed data:")
        print(network_data)

        return network_data

    @task
    def load(network_data):

        print("Final processed network record:")
        print(network_data)

    extracted_data = extract()

    transformed_data = transform(extracted_data)

    load(transformed_data)


first_etl_pipeline()