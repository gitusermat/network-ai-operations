from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum
import requests
from datetime import timedelta


@dag(
    dag_id="weather_etl_pipeline",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "api", "postgres"],
)
def weather_etl_pipeline():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

    def extract():

        url = "https://api.open-meteo.com/v1/forecast"

        params = {
            "latitude": 38.9072,
            "longitude": -77.0369,
            "current": (
                "temperature_2m,"
                "relative_humidity_2m,"
                "wind_speed_10m"
            ),
        }

        response = requests.get(
            url,
            params=params,
            timeout=30,
        )

        response.raise_for_status()

        data = response.json()

        print("Raw API response:")
        print(data)

        return data


    @task
    def transform(data):

        current = data["current"]

        transformed = {
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "observation_time": current["time"],
            "temperature_c": current["temperature_2m"],
            "humidity_percent": current["relative_humidity_2m"],
            "wind_speed": current["wind_speed_10m"],
        }

        print("Transformed record:")
        print(transformed)

        return transformed


    @task
    def validate(data):
        required_fields = [
            "latitude",
            "longitude",
            "observation_time",
            "temperature_c",
            "humidity_percent",
            "wind_speed",
        ]

        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")

        if data["humidity_percent"] < 0 or data["humidity_percent"] > 100:
            raise ValueError("Humidity must be between 0 and 100.")

        if data["temperature_c"] < -100 or data["temperature_c"] > 100:
            raise ValueError("Temperature value is outside the expected range.")

        if data["wind_speed"] < 0:
            raise ValueError("Wind speed cannot be negative.")

        print("Data validation passed.")
        return data


    @task
    def load(data):
        postgres_hook = PostgresHook(
            postgres_conn_id="weather_postgres"
        )

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS weather_observations (
            id SERIAL PRIMARY KEY,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            observation_time TIMESTAMP,
            temperature_c DOUBLE PRECISION,
            humidity_percent DOUBLE PRECISION,
            wind_speed DOUBLE PRECISION,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(latitude, longitude, observation_time)
        );
        """

        postgres_hook.run(create_table_sql)

        insert_sql = """
        INSERT INTO weather_observations (
            latitude,
            longitude,
            observation_time,
            temperature_c,
            humidity_percent,
            wind_speed
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, observation_time)
        DO NOTHING;
        """

        postgres_hook.run(
            insert_sql,
            parameters=(
                data["latitude"],
                data["longitude"],
                data["observation_time"],
                data["temperature_c"],
                data["humidity_percent"],
                data["wind_speed"],
            ),
        )

        print("Weather observation successfully loaded into PostgreSQL.")


    extracted_data = extract()

    transformed_data = transform(extracted_data)

    validated_data = validate(transformed_data)
    
    load(validated_data)


weather_etl_pipeline()