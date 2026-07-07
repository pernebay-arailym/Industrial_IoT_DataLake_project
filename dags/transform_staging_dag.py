from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

import boto3
import pandas as pd
from io import StringIO
import os

from dotenv import load_dotenv


load_dotenv(
    "/Users/pernebayarailym/Documents/Portfolio_Projects_AP/Simplon_DE_Projects/Python_Projects/Industrial_IoT_DataLake_project/.env"
)


s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)



def transform_files():

    lines = [
        "lineA",
        "lineB",
        "lineC",
        "lineD",
        "lineE"
    ]


    for line in lines:

        raw_key = f"production_lines/{line}/year=2026/month=06/line={line}/{line}.csv"


        response = s3.get_object(
            Bucket="raw",
            Key=raw_key
        )


        # LineA processed by chunks
        if line == "lineA":

            chunks = pd.read_csv(
                response["Body"],
                chunksize=1000
            )

            chunk_number = 0


            for chunk in chunks:

                print(f"Processing LineA chunk {chunk_number}")


                chunk.columns = (
                    chunk.columns
                    .str.lower()
                    .str.replace(" ", "_")
                )


                chunk["production_line"] = line


                csv_buffer = StringIO()


                chunk.to_csv(
                    csv_buffer,
                    index=False
                )


                staging_key = (
                    f"production_lines/{line}/"
                    f"batch_{chunk_number}_staging.csv"
                )


                s3.put_object(
                    Bucket="staging",
                    Key=staging_key,
                    Body=csv_buffer.getvalue()
                )


                print(
                    f"Uploaded LineA batch {chunk_number}"
                )


                chunk_number += 1



        else:

            df = pd.read_csv(response["Body"])


            print("Before:")
            print(df.columns)


            df.columns = (
                df.columns
                .str.lower()
                .str.replace(" ", "_")
            )


            df["production_line"] = line


            print("After:")
            print(df.columns)


            csv_buffer = StringIO()


            df.to_csv(
                csv_buffer,
                index=False
            )


            staging_key = (
                f"production_lines/{line}/{line}_staging.csv"
            )


            s3.put_object(
                Bucket="staging",
                Key=staging_key,
                Body=csv_buffer.getvalue()
            )


            print(f"Processed {line}")



with DAG(
    dag_id="transform_raw_to_staging",
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False
) as dag:


    task = PythonOperator(
        task_id="transform_csv_files",
        python_callable=transform_files
    )