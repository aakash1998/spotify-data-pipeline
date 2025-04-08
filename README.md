# 🎧 Spotify ETL Data Pipeline (AWS + Snowflake + PySpark)

This project demonstrates a full **end-to-end, automated data engineering pipeline** built using AWS services, PySpark (Glue), and Snowflake. It extracts data from the **Spotify API**, transforms it using **AWS Glue**, and loads it into a **Snowflake** data warehouse for analytics.

---

## 📊 Project Architecture

```
Spotify API → Lambda → S3 (raw) → Glue Job (ETL) → S3 (transformed) → Snowpipe → Snowflake Tables
```

---

## 🧱 Tech Stack

- **Data Ingestion**: Spotify API, AWS Lambda, Boto3
- **Storage**: AWS S3
- **Data Transformation**: AWS Glue (PySpark)
- **Orchestration**: S3 Trigger + AWS Lambda
- **Data Warehouse**: Snowflake (Snowpipe)
- **Automation**: Entire pipeline is triggered and handled automatically

---

## 📁 Project Structure

```
├── lambda_function/
│   └── spotify_etl_lambda.py         # Lambda function to pull data from Spotify API
├── glue_jobs/
│   └── transform_spotify_data.py     # PySpark ETL script for AWS Glue
├── snowflake/
│   ├── create_tables.sql             # Snowflake table schema
│   └── create_snowpipe.sql           # Snowpipe config for automated loads
├── s3_structure/
│   └── raw_data/
│       └── to_processed/
│       └── processed/
│   └── transformed_data/
│       └── album_data/
│       └── artist_data/
│       └── song_data/
└── README.md
```

---

## 🚀 Pipeline Breakdown

### 1. 🎧 **Spotify API Ingestion with Lambda**

- Extracts recent track data from the Spotify API.
- Lambda function is deployed with environment variables for secure API credentials.
- Data is saved to S3 in `raw_data/to_processed/` as `.json`.

✅ Triggered automatically by **time-based CloudWatch schedule** or **manually**.

---

### 2. 🗂️ **S3 Buckets Structure**

- `raw_data/to_processed/`: Raw incoming Spotify data
- `raw_data/processed/`: Archive of processed raw files
- `transformed_data/`: Final cleaned & transformed data output from Glue

---

### 3. 🧪 **Data Transformation with AWS Glue (PySpark)**

- Glue job reads JSON files from S3.
- Parses, normalizes, and extracts:
  - 🎵 `Songs` (name, duration, popularity, artist, album)
  - 🎨 `Artists` (id, name, external links)
  - 💿 `Albums` (name, release date, track count)
- Transformed data saved as CSV to `transformed_data/` folder in S3.

✅ Fully automated and scalable with PySpark.

---

### 4. ❄️ **Snowflake Integration with Snowpipe**

- Snowflake tables created to store Album, Artist, and Song data.
- Snowpipe listens to the S3 bucket and auto-loads new transformed CSV files.
- Ensures near real-time ingestion into Snowflake for analytics.

✅ Automatically triggered when new files land in S3.

---

## 📦 Example Tables in Snowflake

| Table Name       | Description                        |
|------------------|------------------------------------|
| `spotify_albums` | Album metadata                     |
| `spotify_artists`| Artist metadata                    |
| `spotify_songs`  | Track metadata (linked to artist/album) |

---

## ⚙️ How to Run This Project

### 🔐 Prerequisites

- AWS account with S3, Lambda, Glue access
- Snowflake account
- Spotify Developer API keys
- IAM roles for Glue and Lambda (with proper permissions)

---

### 🧪 Deployment Steps

1. **Set up AWS S3 buckets**
   - Create required folders as per structure above.

2. **Deploy Lambda Function**
   - Use `spotify_etl_lambda.py`
   - Set environment variables for Spotify keys
   - Add CloudWatch trigger (optional)

3. **Create Glue Job**
   - Use `transform_spotify_data.py`
   - Set the script path in the Glue job
   - Schedule via trigger or run on file arrival

4. **Deploy Snowflake Tables + Snowpipe**
   - Use `create_tables.sql` and `create_snowpipe.sql`
   - Ensure Snowpipe has access to your S3 bucket via external stage

5. **Automate Move/Delete**
   - Handled within the Glue job using boto3 after transformation

---

## 🎯 Features

- ✅ Scalable Spark-based ETL
- ✅ Serverless ingestion with Lambda
- ✅ Modular & clean PySpark logic
- ✅ Snowflake integration via Snowpipe
- ✅ Automated cleanup & data lifecycle
- ✅ Real-time-ready analytics layer

---

## 🧠 Author

Aakash Patel
