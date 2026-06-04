import boto3
import os

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

files = [
    ("lineA.csv", "lineA"),
    ("lineB.csv", "lineB"),
    ("lineC.csv", "lineC"),
    ("lineD.csv", "lineD"),
    ("lineE.csv", "lineE")
]

for file_name, line in files:

    s3.upload_file(
        f"./data/{file_name}",
        "raw",
        f"production_lines/{line}/{file_name}"
    )

    print(f"Uploaded {file_name}")