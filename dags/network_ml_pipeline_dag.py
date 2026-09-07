from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.bedrock import BedrockRuntimeHook

import pendulum
import pandas as pd
import numpy as np


# ============================================================
# AWS / FILE CONFIGURATION
# ============================================================

BUCKET_NAME = "bucket1-network-ai-lab-2026"

RAW_S3_KEY = "raw/network_metrics.csv"
SCORED_S3_KEY = "scored/network_metrics_scored.csv"
RCA_S3_KEY = "processed/network_rca_results.csv"

DOWNLOADED_FILE = (
    "/opt/airflow/dags/data/"
    "network_metrics_from_s3.csv"
)

FEATURE_FILE = (
    "/opt/airflow/dags/data/"
    "network_metrics_features.csv"
)

SCORED_FILE = (
    "/opt/airflow/dags/data/"
    "network_metrics_scored.csv"
)

RCA_FILE = (
    "/opt/airflow/dags/data/"
    "network_rca_results.csv"
)

# NEW — local network log file
LOG_FILE = (
    "/opt/airflow/dags/data/"
    "network_logs.csv"
)

# NEW — output after anomalies are matched with logs
CORRELATED_FILE = (
    "/opt/airflow/dags/data/"
    "network_incidents_with_logs.csv"
)

BEDROCK_REGION = "us-east-1"
BEDROCK_MODEL_ID = "amazon.nova-lite-v1:0"


# ============================================================
# DAG DEFINITION
# ============================================================

@dag(
    dag_id="network_ml_pipeline",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=[
        "network",
        "aws",
        "s3",
        "pandas",
        "anomaly-detection",
        "bedrock",
        "genai",
        "rca",
    ],
)
def network_ml_pipeline():

    # ========================================================
    # TASK 1 — EXTRACT FROM AMAZON S3
    # ========================================================

    @task
    def extract_data():

        print("Starting extraction from Amazon S3.")
        print(f"Bucket: {BUCKET_NAME}")
        print(f"S3 key: {RAW_S3_KEY}")

        s3_hook = S3Hook(
            aws_conn_id="aws_default"
        )

        s3_client = s3_hook.get_conn()

        s3_client.download_file(
            BUCKET_NAME,
            RAW_S3_KEY,
            DOWNLOADED_FILE,
        )

        print(
            "File downloaded successfully from S3."
        )

        df = pd.read_csv(
            DOWNLOADED_FILE
        )

        print(f"Rows downloaded: {len(df)}")
        print(f"Columns: {len(df.columns)}")

        print("First five rows:")
        print(df.head())

        return DOWNLOADED_FILE


    # ========================================================
    # TASK 2 — VALIDATE DATA
    # ========================================================

    @task
    def validate_data(input_file):

        print("Starting data validation.")

        df = pd.read_csv(
            input_file
        )

        required_columns = [
            "event_time",
            "device",
            "latency_ms",
            "packet_loss_pct",
            "throughput_mbps",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing columns: {missing_columns}"
            )

        if df.empty:
            raise ValueError(
                "Dataset is empty."
            )

        if df["device"].isna().any():
            raise ValueError(
                "Device column contains missing values."
            )

        if df["latency_ms"].isna().any():
            raise ValueError(
                "Latency contains missing values."
            )

        if df["packet_loss_pct"].isna().any():
            raise ValueError(
                "Packet loss contains missing values."
            )

        if df["throughput_mbps"].isna().any():
            raise ValueError(
                "Throughput contains missing values."
            )

        if (df["latency_ms"] < 0).any():
            raise ValueError(
                "Negative latency detected."
            )

        if (
            (df["packet_loss_pct"] < 0)
            |
            (df["packet_loss_pct"] > 100)
        ).any():
            raise ValueError(
                "Invalid packet loss value detected."
            )

        if (df["throughput_mbps"] < 0).any():
            raise ValueError(
                "Negative throughput detected."
            )

        print("Data validation passed.")
        print(f"Validated rows: {len(df)}")

        return input_file


    # ========================================================
    # TASK 3 — FEATURE ENGINEERING
    # ========================================================

    @task
    def engineer_features(input_file):

        print(
            "Starting feature engineering."
        )

        df = pd.read_csv(
            input_file
        )

        df["event_time"] = pd.to_datetime(
            df["event_time"]
        )

        df = df.sort_values(
            [
                "device",
                "event_time",
            ]
        ).reset_index(drop=True)

        # ====================================================
        # HISTORICAL ROLLING BASELINES
        #
        # shift(1) ensures the current observation is NOT used
        # to calculate its own baseline.
        # ====================================================

        # ----------------------------------------------------
        # Latency baseline
        # ----------------------------------------------------

        df["latency_rolling_mean"] = (
            df.groupby(
                "device"
            )["latency_ms"]
            .transform(
                lambda x:
                x.shift(1)
                .rolling(
                    window=10,
                    min_periods=3,
                )
                .mean()
            )
        )

        df["latency_rolling_std"] = (
            df.groupby(
                "device"
            )["latency_ms"]
            .transform(
                lambda x:
                x.shift(1)
                .rolling(
                    window=10,
                    min_periods=3,
                )
                .std()
            )
        )

        # ----------------------------------------------------
        # Packet-loss baseline
        # ----------------------------------------------------

        df["packet_loss_rolling_mean"] = (
            df.groupby(
                "device"
            )["packet_loss_pct"]
            .transform(
                lambda x:
                x.shift(1)
                .rolling(
                    window=10,
                    min_periods=3,
                )
                .mean()
            )
        )

        df["packet_loss_rolling_std"] = (
            df.groupby(
                "device"
            )["packet_loss_pct"]
            .transform(
                lambda x:
                x.shift(1)
                .rolling(
                    window=10,
                    min_periods=3,
                )
                .std()
            )
        )

        # ----------------------------------------------------
        # Throughput baseline
        # ----------------------------------------------------

        df["throughput_rolling_mean"] = (
            df.groupby(
                "device"
            )["throughput_mbps"]
            .transform(
                lambda x:
                x.shift(1)
                .rolling(
                    window=10,
                    min_periods=3,
                )
                .mean()
            )
        )

        df["throughput_rolling_std"] = (
            df.groupby(
                "device"
            )["throughput_mbps"]
            .transform(
                lambda x:
                x.shift(1)
                .rolling(
                    window=10,
                    min_periods=3,
                )
                .std()
            )
        )

        print(
            "Historical rolling baselines created."
        )

        print(
            df[
                [
                    "event_time",
                    "device",
                    "latency_ms",
                    "latency_rolling_mean",
                    "packet_loss_pct",
                    "packet_loss_rolling_mean",
                    "throughput_mbps",
                    "throughput_rolling_mean",
                ]
            ].head(20)
        )

        df.to_csv(
            FEATURE_FILE,
            index=False,
        )

        print(
            "Feature engineering completed."
        )

        return FEATURE_FILE


    # ========================================================
    # TASK 4 — ANOMALY SCORING
    # ========================================================

    @task
    def score_anomalies(input_file):

        print(
            "Starting anomaly scoring."
        )

        df = pd.read_csv(
            input_file
        )

        # ====================================================
        # PREPARE STANDARD DEVIATIONS
        # ====================================================
        #
        # A standard deviation of zero cannot be used safely
        # as a denominator for a z-score.
        #
        # Replace zero with NaN temporarily.
        # ====================================================

        latency_std = (
            df["latency_rolling_std"]
            .replace(0, np.nan)
        )

        packet_loss_std = (
            df["packet_loss_rolling_std"]
            .replace(0, np.nan)
        )

        throughput_std = (
            df["throughput_rolling_std"]
            .replace(0, np.nan)
        )

        # ====================================================
        # Z-SCORES
        # ====================================================

        df["latency_z"] = (
            (
                df["latency_ms"]
                -
                df["latency_rolling_mean"]
            )
            /
            latency_std
        )

        df["packet_loss_z"] = (
            (
                df["packet_loss_pct"]
                -
                df[
                    "packet_loss_rolling_mean"
                ]
            )
            /
            packet_loss_std
        )

        df["throughput_z"] = (
            (
                df["throughput_mbps"]
                -
                df[
                    "throughput_rolling_mean"
                ]
            )
            /
            throughput_std
        )

        # ====================================================
        # CLEAN INVALID Z-SCORES
        # ====================================================

        z_columns = [
            "latency_z",
            "packet_loss_z",
            "throughput_z",
        ]

        df[z_columns] = (
            df[z_columns]
            .replace(
                [
                    np.inf,
                    -np.inf,
                ],
                np.nan,
            )
            .fillna(0)
        )

        # ====================================================
        # THROUGHPUT DROP SCORE
        # ====================================================
        #
        # Positive throughput z-score means throughput is higher
        # than usual.
        #
        # Negative throughput z-score means throughput dropped.
        #
        # We care about drops, so reverse its sign.
        # ====================================================

        df[
            "throughput_drop_score"
        ] = (
            -df["throughput_z"]
        )

        # ====================================================
        # COMBINED ANOMALY SCORE
        # ====================================================

        df["anomaly_score"] = (
            df["latency_z"]
            .clip(lower=0)
            +
            df["packet_loss_z"]
            .clip(lower=0)
            +
            df["throughput_drop_score"]
            .clip(lower=0)
        )

        # ====================================================
        # ANOMALY LABEL
        # ====================================================

        conditions = [
            df["anomaly_score"] >= 6,
            df["anomaly_score"] >= 3,
        ]

        labels = [
            "ANOMALY",
            "WARNING",
        ]

        df["anomaly_status"] = (
            np.select(
                conditions,
                labels,
                default="NORMAL",
            )
        )

        df.to_csv(
            SCORED_FILE,
            index=False,
        )

        print(
            "Anomaly scoring completed."
        )

        print(
            "Highest anomaly scores:"
        )

        print(
            df[
                [
                    "event_time",
                    "device",
                    "latency_ms",
                    "latency_rolling_mean",
                    "latency_z",
                    "packet_loss_pct",
                    "packet_loss_rolling_mean",
                    "packet_loss_z",
                    "throughput_mbps",
                    "throughput_rolling_mean",
                    "throughput_z",
                    "anomaly_score",
                    "anomaly_status",
                ]
            ]
            .sort_values(
                "anomaly_score",
                ascending=False,
            )
            .head(20)
        )

        return SCORED_FILE


    # ========================================================
    # TASK 5 — UPLOAD SCORED DATA TO AMAZON S3
    # ========================================================

    @task
    def upload_scored_data(input_file):

        print(
            "Uploading scored dataset to Amazon S3."
        )

        s3_hook = S3Hook(
            aws_conn_id="aws_default"
        )

        s3_hook.load_file(
            filename=input_file,
            key=SCORED_S3_KEY,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        destination = (
            f"s3://{BUCKET_NAME}/"
            f"{SCORED_S3_KEY}"
        )

        print("Upload completed successfully.")
        print(f"Destination: {destination}")

        # Return the local file path so downstream tasks
        # can continue reading the scored CSV.
        return input_file


    # ========================================================
    # TASK 6 — LOAD SCORED DATA INTO POSTGRESQL
    # ========================================================

    @task
    def load_to_postgres(input_file):

        print("Starting PostgreSQL load.")

        df = pd.read_csv(
            input_file
        )

        postgres_hook = PostgresHook(
            postgres_conn_id="weather_postgres"
        )

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS network_ml_events (

            id SERIAL PRIMARY KEY,

            event_time TIMESTAMP NOT NULL,

            device VARCHAR(100) NOT NULL,

            latency_ms DOUBLE PRECISION,

            packet_loss_pct DOUBLE PRECISION,

            throughput_mbps DOUBLE PRECISION,

            latency_rolling_mean DOUBLE PRECISION,

            packet_loss_rolling_mean DOUBLE PRECISION,

            throughput_rolling_mean DOUBLE PRECISION,

            latency_z DOUBLE PRECISION,

            packet_loss_z DOUBLE PRECISION,

            throughput_z DOUBLE PRECISION,

            anomaly_score DOUBLE PRECISION,

            anomaly_status VARCHAR(20),

            loaded_at TIMESTAMP
                DEFAULT CURRENT_TIMESTAMP,

            UNIQUE (
                event_time,
                device
            )
        );
        """

        postgres_hook.run(
            create_table_sql
        )

        insert_sql = """
        INSERT INTO network_ml_events (

            event_time,
            device,

            latency_ms,
            packet_loss_pct,
            throughput_mbps,

            latency_rolling_mean,
            packet_loss_rolling_mean,
            throughput_rolling_mean,

            latency_z,
            packet_loss_z,
            throughput_z,

            anomaly_score,
            anomaly_status
        )

        VALUES (
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s
        )

        ON CONFLICT (
            event_time,
            device
        )

        DO UPDATE SET

            latency_ms =
                EXCLUDED.latency_ms,

            packet_loss_pct =
                EXCLUDED.packet_loss_pct,

            throughput_mbps =
                EXCLUDED.throughput_mbps,

            latency_rolling_mean =
                EXCLUDED.latency_rolling_mean,

            packet_loss_rolling_mean =
                EXCLUDED.packet_loss_rolling_mean,

            throughput_rolling_mean =
                EXCLUDED.throughput_rolling_mean,

            latency_z =
                EXCLUDED.latency_z,

            packet_loss_z =
                EXCLUDED.packet_loss_z,

            throughput_z =
                EXCLUDED.throughput_z,

            anomaly_score =
                EXCLUDED.anomaly_score,

            anomaly_status =
                EXCLUDED.anomaly_status;
        """

        connection = (
            postgres_hook.get_conn()
        )

        cursor = (
            connection.cursor()
        )

        for _, row in df.iterrows():

            cursor.execute(
                insert_sql,
                (
                    row["event_time"],
                    row["device"],

                    float(
                        row["latency_ms"]
                    ),

                    float(
                        row["packet_loss_pct"]
                    ),

                    float(
                        row["throughput_mbps"]
                    ),

                    float(
                        row["latency_rolling_mean"]
                    ),

                    float(
                        row["packet_loss_rolling_mean"]
                    ),

                    float(
                        row["throughput_rolling_mean"]
                    ),

                    float(
                        row["latency_z"]
                    ),

                    float(
                        row["packet_loss_z"]
                    ),

                    float(
                        row["throughput_z"]
                    ),

                    float(
                        row["anomaly_score"]
                    ),

                    row["anomaly_status"],
                ),
            )

        connection.commit()

        cursor.close()
        connection.close()

        print(
            f"Loaded {len(df)} records "
            "into PostgreSQL."
        )

        return input_file


    # ========================================================
    # TASK 7 — CORRELATE ANOMALIES WITH NETWORK LOGS
    # ========================================================

    @task
    def correlate_logs(input_file):

        print("Starting anomaly-to-log correlation.")

        incidents_df = pd.read_csv(input_file)
        logs_df = pd.read_csv(LOG_FILE)

        incidents_df["event_time"] = pd.to_datetime(incidents_df["event_time"])
        logs_df["event_time"] = pd.to_datetime(logs_df["event_time"])

        incidents = (
            incidents_df[incidents_df["anomaly_status"].isin(["ANOMALY", "WARNING"])]
            .sort_values("anomaly_score", ascending=False)
            .head(10)
            .copy()
        )

        correlated_results = []

        for _, incident in incidents.iterrows():
            incident_time = incident["event_time"]
            device = incident["device"]
            window_start = incident_time - pd.Timedelta(minutes=5)
            window_end = incident_time + pd.Timedelta(minutes=5)

            matching_logs = logs_df[
                (logs_df["device"] == device)
                & (logs_df["event_time"] >= window_start)
                & (logs_df["event_time"] <= window_end)
            ].sort_values("event_time")

            if matching_logs.empty:
                log_context = "No related logs found within the +/- 5 minute window."
            else:
                log_lines = []
                for _, log in matching_logs.iterrows():
                    log_lines.append(
                        f'{log["event_time"]} | {log["severity"]} | '
                        f'{log["event_type"]} | {log["message"]}'
                    )
                log_context = "\n".join(log_lines)

            correlated_results.append({
                "event_time": incident["event_time"],
                "device": device,
                "latency_ms": incident["latency_ms"],
                "packet_loss_pct": incident["packet_loss_pct"],
                "throughput_mbps": incident["throughput_mbps"],
                "latency_rolling_mean": incident["latency_rolling_mean"],
                "packet_loss_rolling_mean": incident["packet_loss_rolling_mean"],
                "throughput_rolling_mean": incident["throughput_rolling_mean"],
                "anomaly_score": incident["anomaly_score"],
                "anomaly_status": incident["anomaly_status"],
                "related_logs": log_context,
            })

        result_df = pd.DataFrame(correlated_results)
        result_df.to_csv(CORRELATED_FILE, index=False)

        print(f"Created {len(result_df)} correlated incidents.")
        print(f"Correlation output: {CORRELATED_FILE}")
        print("Log correlation completed.")

        return CORRELATED_FILE


    # ========================================================
    # TASK 8 — BEDROCK LLM ROOT-CAUSE ANALYSIS
    # ========================================================

    @task
    def analyze_anomalies_with_llm(input_file):

        print(
            "Starting LLM-assisted "
            "root-cause analysis."
        )

        df = pd.read_csv(
            input_file
        )

        incidents = (
            df[
                df["anomaly_status"].isin(
                    [
                        "ANOMALY",
                        "WARNING",
                    ]
                )
            ]
            .sort_values(
                "anomaly_score",
                ascending=False,
            )
            .head(10)
            .copy()
        )

        print(
            f"Selected {len(incidents)} "
            "high-priority incidents."
        )

        # Always create an output file so downstream
        # tasks have a valid path even if no incidents exist.
        if incidents.empty:

            empty_rca_df = pd.DataFrame(
                columns=[
                    "event_time",
                    "device",
                    "anomaly_score",
                    "anomaly_status",
                    "ai_rca",
                ]
            )

            empty_rca_df.to_csv(
                RCA_FILE,
                index=False,
            )

            print(
                "No incidents require LLM analysis."
            )

            return RCA_FILE

        bedrock_hook = BedrockRuntimeHook(
            aws_conn_id="aws_default",
            region_name=BEDROCK_REGION,
        )

        client = bedrock_hook.get_conn()

        rca_results = []

        for _, row in incidents.iterrows():

            prompt = f"""
You are an AI assistant supporting a network operations engineer.

Analyze the following network incident using both telemetry metrics
and the related operational logs.

Device:
{row["device"]}

Time:
{row["event_time"]}

Current metrics:
Latency: {row["latency_ms"]} ms
Packet loss: {row["packet_loss_pct"]} percent
Throughput: {row["throughput_mbps"]} Mbps

Recent historical baseline:
Latency mean: {row["latency_rolling_mean"]} ms
Packet-loss mean: {row["packet_loss_rolling_mean"]} percent
Throughput mean: {row["throughput_rolling_mean"]} Mbps

Anomaly score:
{row["anomaly_score"]}

Related network logs:
{row["related_logs"]}

Return:
1. A short incident summary
2. Three root-cause hypotheses ranked by plausibility
3. For each hypothesis, cite the specific telemetry and log evidence
4. Evidence that contradicts or weakens each hypothesis
5. Additional evidence still needed
6. Recommended investigation or remediation steps

Important rules:
- Distinguish observations from hypotheses.
- Give greater weight to timestamped logs that directly precede the anomaly.
- Do not claim a root cause is confirmed unless the evidence is sufficient.
- Do not invent logs, metrics, configuration changes, or device states.
- Keep the response technical and concise.
"""

            response = client.converse(
                modelId=BEDROCK_MODEL_ID,

                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "text": prompt
                            }
                        ],
                    }
                ],

                inferenceConfig={
                    "maxTokens": 600,
                    "temperature": 0.2,
                    "topP": 0.9,
                },
            )

            ai_analysis = (
                response["output"]
                ["message"]
                ["content"][0]
                ["text"]
            )

            rca_results.append(
                {
                    "event_time":
                        row["event_time"],

                    "device":
                        row["device"],

                    "anomaly_score":
                        float(
                            row["anomaly_score"]
                        ),

                    "anomaly_status":
                        row["anomaly_status"],

                    "ai_rca":
                        ai_analysis,
                }
            )

        rca_df = pd.DataFrame(
            rca_results
        )

        rca_df.to_csv(
            RCA_FILE,
            index=False,
        )

        print(
            f"Created {len(rca_df)} "
            "RCA records."
        )

        return RCA_FILE


    # ========================================================
    # TASK 9 — UPLOAD RCA RESULTS TO AMAZON S3
    # ========================================================

    @task
    def upload_rca_to_s3(input_file):

        print(
            "Uploading RCA results to Amazon S3."
        )

        s3_hook = S3Hook(
            aws_conn_id="aws_default"
        )

        s3_hook.load_file(
            filename=input_file,
            key=RCA_S3_KEY,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        destination = (
            f"s3://{BUCKET_NAME}/"
            f"{RCA_S3_KEY}"
        )

        print(
            "RCA upload completed successfully."
        )

        print(
            f"Destination: {destination}"
        )

        return input_file


    # ========================================================
    # TASK 10 — LOAD RCA RESULTS INTO POSTGRESQL
    # ========================================================

    @task
    def load_rca_to_postgres(input_file):

        print(
            "Starting RCA PostgreSQL load."
        )

        df = pd.read_csv(
            input_file
        )

        postgres_hook = PostgresHook(
            postgres_conn_id="weather_postgres"
        )

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS network_rca_results (

            id SERIAL PRIMARY KEY,

            event_time TIMESTAMP NOT NULL,

            device VARCHAR(100) NOT NULL,

            anomaly_score DOUBLE PRECISION,

            anomaly_status VARCHAR(20),

            ai_rca TEXT,

            created_at TIMESTAMP
                DEFAULT CURRENT_TIMESTAMP,

            UNIQUE (
                event_time,
                device
            )
        );
        """

        postgres_hook.run(
            create_table_sql
        )

        # If there were no WARNING/ANOMALY rows,
        # there is nothing to insert.
        if df.empty:

            print(
                "RCA file is empty. "
                "Nothing to load into PostgreSQL."
            )

            return input_file

        insert_sql = """
        INSERT INTO network_rca_results (

            event_time,
            device,
            anomaly_score,
            anomaly_status,
            ai_rca
        )

        VALUES (
            %s, %s, %s, %s, %s
        )

        ON CONFLICT (
            event_time,
            device
        )

        DO UPDATE SET

            anomaly_score =
                EXCLUDED.anomaly_score,

            anomaly_status =
                EXCLUDED.anomaly_status,

            ai_rca =
                EXCLUDED.ai_rca,

            created_at =
                CURRENT_TIMESTAMP;
        """

        connection = (
            postgres_hook.get_conn()
        )

        cursor = (
            connection.cursor()
        )

        for _, row in df.iterrows():

            cursor.execute(
                insert_sql,
                (
                    row["event_time"],
                    row["device"],
                    float(
                        row["anomaly_score"]
                    ),
                    row["anomaly_status"],
                    row["ai_rca"],
                ),
            )

        connection.commit()

        cursor.close()
        connection.close()

        print(
            f"Loaded {len(df)} RCA records "
            "into PostgreSQL."
        )

        return input_file


    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    raw_file = extract_data()

    validated_file = validate_data(
        raw_file
    )

    feature_file = engineer_features(
        validated_file
    )

    scored_file = score_anomalies(
        feature_file
    )

    uploaded_scored_file = upload_scored_data(
        scored_file
    )

    # Store the scored telemetry in PostgreSQL.
    load_to_postgres(
        uploaded_scored_file
    )

    # Correlate high-priority telemetry incidents with nearby
    # operational logs from the same device.
    correlated_file = correlate_logs(
        uploaded_scored_file
    )

    # Send correlated telemetry + log evidence to Bedrock.
    rca_file = analyze_anomalies_with_llm(
        correlated_file
    )

    # Persist AI-generated RCA results in S3.
    uploaded_rca_file = upload_rca_to_s3(
        rca_file
    )

    # Persist AI-generated RCA results in PostgreSQL.
    load_rca_to_postgres(
        uploaded_rca_file
    )


# ============================================================
# REGISTER DAG
# ============================================================

network_ml_pipeline()