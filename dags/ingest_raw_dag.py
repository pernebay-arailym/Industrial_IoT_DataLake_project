from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os
from dotenv import load_dotenv

#load_dotenv(
#    "/Users/pernebayarailym/Documents/Portfolio_Projects_AP/Simplon_DE_Projects/Python_Projects/Industrial_IoT_DataLake_project/.env"
#)

print("Current working directory:", os.getcwd())
print("Dotenv loaded:", load_dotenv("/Users/pernebayarailym/Documents/Portfolio_Projects_AP/Simplon_DE_Projects/Python_Projects/Industrial_IoT_DataLake_project/.env"))
print("ACCESS KEY:", os.getenv("AWS_ACCESS_KEY_ID"))
print("SECRET KEY:", os.getenv("AWS_SECRET_ACCESS_KEY"))

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

print(os.getenv("AWS_ACCESS_KEY_ID"))
print(os.getenv("AWS_SECRET_ACCESS_KEY"))


#DATA_PATH = os.path.join(os.getcwd(), "data")
#"/path/to/project/data"
#BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#DATA_PATH = os.path.join(BASE_DIR, "data")
DATA_PATH = "/Users/pernebayarailym/Documents/Portfolio_Projects_AP/Simplon_DE_Projects/Python_Projects/Industrial_IoT_DataLake_project/data"

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

    task