# Industrial IoT DataLake Project

## Context

This project implements a Data Lake architecture for an industrial IoT environment.

The objective is to centralize and structure sensor data coming from 5 production lines:

* LineA
* LineB
* LineC
* LineD
* LineE

The original CSV files contain heterogeneous schemas and are stored without organization. The Data Lake architecture introduces different layers:

```
                Industrial IoT Sources
                         |
                         |
                         v

                    RAW Layer
             (Original immutable data)
                         |
                         |
                         v

                 STAGING Layer
        (Cleaned and standardized data)
                         |
                         |
                         v

                 CURATED Layer
          (Business-ready analytical data)

                         |
                         |
                         v

                 ARCHIVE Layer
            (Historical / expired data)
```

Technologies used:

* MinIO (S3-compatible object storage)
* Docker
* Apache Airflow
* Python
* boto3
* Pandas

---

# 1. Project Structure

```
Industrial_IoT_DataLake_project/

│
├── data/
│   ├── lineA.csv
│   ├── lineB.csv
│   ├── lineC.csv
│   ├── lineD.csv
│   └── lineE.csv
│
├── dags/
│   ├── ingest_raw_dag.py
│   └── transform_staging_dag.py
│
├── scripts/
│   ├── upload_to_minio.py
│   └── verify_integrity.py
│
├── screenshots/
│
├── docker-compose.yml
│
├── .env
│
└── README.md
```

---

# 2. MinIO Installation

MinIO is used as the object storage layer of the Data Lake.

Start MinIO with Docker:

```bash
docker compose up -d
```

Verify containers:

```bash
docker ps
```

MinIO services:

* API endpoint:

```
http://localhost:9000
```

* Web Console:

```
http://localhost:9001
```

Login credentials:

```
username: minioadmin
password: minioadmin
```

---

# 3. Data Lake Buckets

Four buckets were created:

```
raw
staging
curated
archive
```

Their responsibilities:

## raw

Contains original source data.

Characteristics:

* Immutable
* No modification
* Exact copy from production sources

Example:

```
raw/

production_lines/

└── lineA/

    └── year=2026/

        └── month=06/

            └── line=lineA/

                └── lineA.csv
```

## staging

Contains transformed data:

* standardized column names
* cleaned formats
* additional metadata

## curated

Contains data prepared for analytics and future machine learning.

## archive

Contains historical data according to lifecycle policies.

---

# 4. Environment Configuration

Create a `.env` file:

```env
AWS_ACCESS_KEY_ID=minioadmin

AWS_SECRET_ACCESS_KEY=minioadmin
```

These credentials are used by boto3 to communicate with MinIO.

---

# 5. Upload CSV Files to MinIO

The upload process uses Python boto3.

Run:

```bash
python scripts/upload_to_minio.py
```

The script uploads:

```
lineA.csv
lineB.csv
lineC.csv
lineD.csv
lineE.csv
```

into the RAW bucket.

The final structure:

```
raw/

production_lines/

├── lineA/
│
│── year=2026/
│
│── month=06/
│
│── line=lineA/

├── lineB/
├── lineC/
├── lineD/
└── lineE/
```

---

# 6. Data Integrity Verification (MD5)

To verify that uploaded files are not corrupted, MD5 hashes are compared.

A hash is a unique fingerprint generated from a file.

If two files have the same MD5 value:

```
original file MD5
        =
uploaded file MD5
```

the content is identical.

Run:

```bash
python scripts/verify_integrity.py
```

Expected result:

```
lineA.csv OK
lineB.csv OK
lineC.csv OK
lineD.csv OK
lineE.csv OK
```

---

# 7. Airflow Installation and Configuration

Airflow is used to orchestrate the Data Lake pipelines.

Initialize Airflow:

```bash
airflow db init
```

Create an administrator account:

```bash
airflow users create \
--username admin \
--firstname Admin \
--lastname User \
--role Admin \
--email admin@example.com \
--password admin
```

Start Airflow:

Terminal 1:

```bash
airflow webserver
```

Terminal 2:

```bash
airflow scheduler
```

Access:

```
http://localhost:8080
```

---

# 8. DAG 1 - Raw Ingestion Pipeline

DAG name:

```
ingest_raw_minio
```

Purpose:

Automatically ingest CSV files into the RAW layer.

Workflow:

```
CSV Files

   |
   |
   v

Airflow PythonOperator

   |
   |
   v

boto3 upload

   |
   |
   v

MinIO RAW bucket
```

The DAG creates a partitioned structure:

```
production_lines/{line}/year={year}/month={month}/line={line}/file.csv
```

Example:

```
raw/

production_lines/

lineA/

year=2026/

month=06/

line=lineA/

lineA.csv
```

Execution:

From Airflow UI:

```
DAGs
 |
 ingest_raw_minio
 |
 Trigger DAG
```

Successful execution uploads all five production line files.

---

# 9. DAG 2 - Transformation Pipeline

DAG name:

```
transform_raw_to_staging
```

Purpose:

Transform RAW data into STAGING data.

Transformations applied:

## 1. Column normalization

Example:

Before:

```
Temperature
Pressure
Elapsed Time
```

After:

```
temperature
pressure
elapsed_time
```

Logic:

```python
df.columns.str.lower()
df.columns.str.replace(" ","_")
```

---

## 2. Metadata enrichment

A new column is added:

```
production_line
```

Example:

Before:

```
temperature
pressure
timestamp
```

After:

```
temperature
pressure
timestamp
production_line
```

This allows identification of the production source after combining multiple datasets.

---

# 10. LineA Batch Processing

LineA contains:

```
10 000 records
```

To simulate a real industrial data stream, LineA is processed by chunks.

Instead of loading:

```
10 000 rows
        |
        v
      RAM
```

the pipeline processes:

```
1000 rows
1000 rows
1000 rows
...
```

using pandas:

```python
pd.read_csv(
    file,
    chunksize=1000
)
```

Each chunk is transformed independently and uploaded into staging.

Example:

```
staging/

lineA/

batch_0_staging.csv

batch_1_staging.csv

batch_2_staging.csv

...
```

This approach allows the pipeline to scale for larger industrial datasets.

---

# 11. Validation

After DAG execution:

Check Airflow:

```
DAG status = SUCCESS
```

Check MinIO:

RAW:

```
production_lines/
```

STAGING:

```
production_lines/
```

with transformed CSV files.

Verify:

* files exist
* partition structure is correct
* columns are normalized
* LineA batches are created

---

# 12. Conclusion

The C19 implementation provides a functional industrial Data Lake foundation:

* Object storage deployed with MinIO
* Raw data ingestion automated with Airflow
* Partitioned data organization implemented
* Data transformation pipeline created
* Schema differences standardized
* Large dataset processing simulated with chunking
* Data integrity verified using MD5

Future improvements:

* OpenMetadata catalog integration
* MinIO lifecycle policies
* Access control policies
* Data encryption
* Governance documentation
