from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os


s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)


DATA_PATH = "/path/to/project/data"


def upload_files():

    files = [
        ("lineA.csv", "lineA"),
        ("lineB.csv", "lineB"),
        ("lineC.csv", "lineC"),
        ("lineD.csv", "lineD"),
        ("lineE.csv", "lineE")
    ]

    for file_name, line in files:

        file_path = os.path.join(DATA_PATH, file_name)

        key = f"production_lines/{line}/year=2026/month=06/line={line}/{file_name}"

        s3.upload_file(file_path, "raw", key)

        print(f"Uploaded {file_name} → {key}")


with DAG(
    dag_id="ingest_raw_minio",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="upload_to_raw",
        python_callable=upload_files
    )