from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pendulum


BUCKET_NAME = "bucket1-network-ai-lab-2026"


@dag(
    dag_id="s3_connection_test",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=[
        "aws",
        "s3",
        "test",
    ],
)
def s3_connection_test():

    @task
    def list_s3_objects():

        print("Connecting to Amazon S3...")

        s3_hook = S3Hook(
            aws_conn_id="aws_default"
        )

        keys = s3_hook.list_keys(
            bucket_name=BUCKET_NAME,
            prefix="raw/",
        )

        print("Objects found in raw/:")

        if not keys:
            print("No objects found.")

        else:
            for key in keys:
                print(key)

        return keys

    list_s3_objects()


s3_connection_test()