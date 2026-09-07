from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum
from datetime import timedelta, datetime
import re


LOG_FILE_PATH = "/opt/airflow/dags/data/network_logs.txt"


@dag(
    dag_id="network_log_etl_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["network", "etl", "logs", "anomaly"],
)
def network_log_etl_pipeline():

    # ---------------------------------------------------------
    # TASK 1: EXTRACT
    # ---------------------------------------------------------

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def extract_logs():

        print(f"Reading network logs from: {LOG_FILE_PATH}")

        with open(LOG_FILE_PATH, "r") as file:
            lines = file.readlines()

        logs = []

        for line in lines:
            cleaned_line = line.strip()

            if cleaned_line:
                logs.append(cleaned_line)

        print(f"Extracted {len(logs)} log records.")

        for log in logs:
            print(log)

        return logs


    # ---------------------------------------------------------
    # TASK 2: PARSE / TRANSFORM
    # ---------------------------------------------------------

    @task
    def parse_logs(logs):

        parsed_logs = []

        log_pattern = re.compile(
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) "
            r"(\S+) "
            r"(\S+) "
            r"latency=(\d+(?:\.\d+)?) "
            r"packet_loss=(\d+(?:\.\d+)?)"
        )

        for log in logs:

            match = log_pattern.fullmatch(log)

            if not match:
                print(f"Could not parse log: {log}")
                continue

            event_time = match.group(1)
            device = match.group(2)
            severity = match.group(3)
            latency = float(match.group(4))
            packet_loss = float(match.group(5))

            parsed_record = {
                "event_time": event_time,
                "device": device,
                "severity": severity,
                "latency_ms": latency,
                "packet_loss_pct": packet_loss,
            }

            parsed_logs.append(parsed_record)

        print(f"Successfully parsed {len(parsed_logs)} records.")

        for record in parsed_logs:
            print(record)

        return parsed_logs


    # ---------------------------------------------------------
    # TASK 3: VALIDATE
    # ---------------------------------------------------------

    @task
    def validate_logs(records):

        if not records:
            raise ValueError("No valid network records were parsed.")

        valid_severities = {
            "INFO",
            "WARNING",
            "ERROR",
        }

        for record in records:

            if not record["device"]:
                raise ValueError("Device name cannot be empty.")

            if record["severity"] not in valid_severities:
                raise ValueError(
                    f"Invalid severity: {record['severity']}"
                )

            if record["latency_ms"] < 0:
                raise ValueError(
                    "Latency cannot be negative."
                )

            if (
                record["packet_loss_pct"] < 0
                or record["packet_loss_pct"] > 100
            ):
                raise ValueError(
                    "Packet loss must be between 0 and 100."
                )

            try:
                datetime.strptime(
                    record["event_time"],
                    "%Y-%m-%d %H:%M:%S",
                )

            except ValueError:
                raise ValueError(
                    f"Invalid timestamp: {record['event_time']}"
                )

        print(
            f"Validation passed for {len(records)} records."
        )

        return records


    # ---------------------------------------------------------
    # TASK 4: ANOMALY SCORING
    # ---------------------------------------------------------

    @task
    def score_anomalies(records):

        scored_records = []

        for record in records:

            latency = record["latency_ms"]
            packet_loss = record["packet_loss_pct"]
            severity = record["severity"]

            if (
                severity == "ERROR"
                or latency >= 250
                or packet_loss >= 5
            ):
                anomaly_status = "ANOMALY"
                anomaly_score = 2

            elif (
                severity == "WARNING"
                or latency >= 150
                or packet_loss >= 2
            ):
                anomaly_status = "WARNING"
                anomaly_score = 1

            else:
                anomaly_status = "NORMAL"
                anomaly_score = 0

            record["anomaly_status"] = anomaly_status
            record["anomaly_score"] = anomaly_score

            scored_records.append(record)

        print("Anomaly scoring completed.")

        for record in scored_records:
            print(record)

        return scored_records


    # ---------------------------------------------------------
    # TASK 5: LOAD INTO POSTGRESQL
    # ---------------------------------------------------------

    @task
    def load_to_postgres(records):

        postgres_hook = PostgresHook(
            postgres_conn_id="weather_postgres"
        )

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS network_log_events (
            id SERIAL PRIMARY KEY,
            event_time TIMESTAMP NOT NULL,
            device VARCHAR(100) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            latency_ms DOUBLE PRECISION NOT NULL,
            packet_loss_pct DOUBLE PRECISION NOT NULL,
            anomaly_status VARCHAR(20) NOT NULL,
            anomaly_score INTEGER NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE (
                event_time,
                device,
                severity,
                latency_ms,
                packet_loss_pct
            )
        );
        """

        postgres_hook.run(create_table_sql)

        insert_sql = """
        INSERT INTO network_log_events (
            event_time,
            device,
            severity,
            latency_ms,
            packet_loss_pct,
            anomaly_status,
            anomaly_score
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)

        ON CONFLICT (
            event_time,
            device,
            severity,
            latency_ms,
            packet_loss_pct
        )

        DO UPDATE SET
            anomaly_status = EXCLUDED.anomaly_status,
            anomaly_score = EXCLUDED.anomaly_score;
        """

        for record in records:

            postgres_hook.run(
                insert_sql,
                parameters=(
                    record["event_time"],
                    record["device"],
                    record["severity"],
                    record["latency_ms"],
                    record["packet_loss_pct"],
                    record["anomaly_status"],
                    record["anomaly_score"],
                ),
            )

        print(
            f"Loaded {len(records)} network records into PostgreSQL."
        )


    # ---------------------------------------------------------
    # PIPELINE DEPENDENCIES
    # ---------------------------------------------------------

    extracted_logs = extract_logs()

    parsed_logs = parse_logs(extracted_logs)

    validated_logs = validate_logs(parsed_logs)

    scored_logs = score_anomalies(validated_logs)

    load_to_postgres(scored_logs)


network_log_etl_pipeline()