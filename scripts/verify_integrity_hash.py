import boto3
import hashlib
import os


# Connect to MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)


def calculate_md5(file_path):
    """
    Calculate the MD5 hash of a local file.
    """
    md5_hash = hashlib.md5()

    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(4096), b""):
            md5_hash.update(chunk)

    return md5_hash.hexdigest()


# Files to verify
files = [
    ("lineA.csv", "lineA"),
    ("lineB.csv", "lineB"),
    ("lineC.csv", "lineC"),
    ("lineD.csv", "lineD"),
    ("lineE.csv", "lineE")
]

print("\n=== FILE INTEGRITY VERIFICATION ===\n")

for file_name, line in files:

    local_path = os.path.join(".", "data", file_name)

    # Local MD5
    local_md5 = calculate_md5(local_path)

    # MinIO ETag
    response = s3.head_object(
        Bucket="raw",
        Key=f"production_lines/{line}/{file_name}"
    )

    etag = response["ETag"].replace('"', "")

    print(f"File: {file_name}")
    print(f"Local MD5 : {local_md5}")
    print(f"MinIO ETag: {etag}")

    if local_md5 == etag:
        print("Integrity verified\n")
    else:
        print("Integrity check failed\n")