import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
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