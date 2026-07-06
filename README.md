# 🚗 Vehicle Batch Orchestrator

> **Production-grade batch processing pipeline** built with **Apache Airflow 3.2**, **Docker**, **AWS S3**, and **FastAPI** that ingests vehicle data from CSV, enriches each record via parallel API calls (RC, FASTag, VRN, Valuation), and produces a consolidated report — all orchestrated with dynamic task mapping.

---

## 📑 Table of Contents

- [Overview](#overview)
- [Architecture Diagram](#architecture-diagram)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Step-by-Step Pipeline Workflow](#step-by-step-pipeline-workflow)
- [Setup — Local Development](#setup--local-development)
- [Setup — AWS Deployment (EC2)](#setup--aws-deployment-ec2)
- [Airflow Variables & Configuration](#airflow-variables--configuration)
- [Mock API Reference](#mock-api-reference)
- [CI/CD — GitHub Actions](#cicd--github-actions)
- [Sample Data](#sample-data)
- [Troubleshooting](#troubleshooting)

---

## Overview

The **Vehicle Batch Orchestrator** is an end-to-end data enrichment pipeline designed for the Indian automotive domain. Given a CSV of vehicles (ID, brand, model, year, fuel type), the pipeline:

1. Downloads the input CSV from **AWS S3**
2. Validates & cleans the data (removes duplicates, nulls, checks schema)
3. Splits records into configurable batch chunks
4. **Enriches each vehicle in parallel** by calling 4 external APIs concurrently:
   - **RC API** — Registration Certificate / Ownership status
   - **FASTag API** — Electronic toll-collection tag status
   - **VRN API** — Vehicle Registration Number blacklist check
   - **Valuation API** — Market price estimation (toggleable)
5. Merges all enriched batches into a final consolidated report
6. Uploads the final report (and any failure records) back to **S3**

The pipeline uses **Airflow's Dynamic Task Mapping** (`expand()`) to create one parallel Celery task per batch chunk — making it horizontally scalable.

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          APACHE AIRFLOW (Docker)                            │
│                                                                              │
│  ┌────────────┐    ┌──────────────┐    ┌───────────────────┐                │
│  │   AWS S3   │───▶│ load_vehicle  │───▶│   split_batches   │                │
│  │  (Input)   │    │   _data()     │    │  (chunking into   │                │
│  └────────────┘    │              │    │   N batch files)  │                │
│                    │ • download   │    └─────────┬─────────┘                │
│                    │ • validate   │              │                          │
│                    │ • clean      │              ▼                          │
│                    └──────────────┘    ┌─────────────────────┐              │
│                                       │  process_batch_task  │              │
│                                       │  .expand()           │              │
│                                       │                      │              │
│                                       │  batch_1 ──┐         │              │
│                                       │  batch_2 ──┤ parallel│              │
│                                       │  batch_N ──┘         │              │
│                                       └─────────┬───────────┘              │
│                                                 │                          │
│                                                 ▼                          │
│  ┌────────────┐    ┌──────────────┐    ┌───────────────────┐              │
│  │   AWS S3   │◀───│   merge_     │◀───│ pipeline_         │              │
│  │  (Output)  │    │   batches()  │    │ completed()       │              │
│  │  (Failed)  │    └──────────────┘    └───────────────────┘              │
│  └────────────┘                                                            │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                    For each vehicle (4 concurrent threads)
                                    │
              ┌─────────┬───────────┼───────────┬────────────┐
              ▼         ▼           ▼           ▼            │
         ┌────────┐ ┌────────┐ ┌────────┐ ┌──────────┐      │
         │ RC API │ │FASTag  │ │VRN API │ │Valuation │      │
         │        │ │  API   │ │        │ │   API    │      │
         └────────┘ └────────┘ └────────┘ └──────────┘      │
              ▲         ▲           ▲           ▲            │
              └─────────┴───────────┴───────────┘            │
                  Mock FastAPI Server (port 8000)            │
                                                             │
```

---

## Tech Stack

| Layer             | Technology                                         |
| ----------------- | -------------------------------------------------- |
| **Orchestrator**  | Apache Airflow 3.2.2 (CeleryExecutor)              |
| **Task Queue**    | Redis 7.2                                          |
| **Metadata DB**   | PostgreSQL 16                                      |
| **Containerization** | Docker & Docker Compose                         |
| **Cloud Storage** | AWS S3 (via `apache-airflow-providers-amazon`)     |
| **Mock APIs**     | FastAPI + Uvicorn (Dockerized)                     |
| **Language**      | Python 3.10+                                       |
| **Data Processing** | Pandas                                           |
| **Retry Logic**   | Tenacity (exponential backoff)                     |
| **CI/CD**         | GitHub Actions (SSH deploy to EC2)                 |
| **Monitoring**    | Celery Flower (optional profile)                   |

---

## Project Structure

```
vehicle-batch-orchestrator/
│
├── .github/
│   └── workflows/
│       └── deploy.yml              # CI/CD: auto-deploy to EC2 on push to main
│
├── config/
│   └── airflow.cfg                 # Custom Airflow configuration
│
├── configs/                        # Python configuration modules
│   ├── __init__.py
│   ├── environment.py              # Staging/Production URL mapping
│   ├── pipeline_config.py          # S3 bucket, paths, chunk sizes
│   └── pipeline_state.py           # Pipeline state constants (RUNNING/PAUSED/CANCELLED)
│
├── dags/
│   └── vehicle_pipeline.py         # ★ Main Airflow DAG — the orchestration entry point
│
├── data/
│   └── input/
│       └── vehicle_data.csv        # Sample input CSV (5 vehicles)
│
├── mock-api/                       # Standalone FastAPI mock server
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                      # FastAPI application
│   ├── routers/                    # API route handlers
│   │   ├── rc.py                   # POST /api/rc
│   │   ├── fastag.py              # POST /api/fastag
│   │   ├── vrn.py                 # POST /api/vrn
│   │   └── valuation.py           # POST /api/valuation
│   ├── schemas/
│   │   └── vehicle.py              # Pydantic request/response models
│   └── services/                   # Business logic for each API
│       ├── rc_service.py           # Returns owner + registration status (30% failure rate)
│       ├── fastag_service.py       # Returns FASTag status + bank
│       ├── vrn_service.py          # Returns blacklist status
│       └── valuation_service.py    # Calculates market price based on vehicle age
│
├── scripts/                        # Pipeline task logic
│   ├── __init__.py
│   ├── api_client.py               # HTTP client with Tenacity retry (3 attempts, exponential backoff)
│   ├── check_pipeline_state.py     # Guards pipeline execution via Airflow Variable
│   ├── csv_reader.py               # Reads CSV into Pandas DataFrame
│   ├── enrich_vehicle.py           # Calls all 4 APIs concurrently (ThreadPoolExecutor)
│   ├── environment_selector.py     # Maps environment name → base URL
│   ├── failed_batches.py           # In-memory failed record tracker
│   ├── process_batch.py            # Processes a single batch file end-to-end
│   ├── s3_downloader.py            # Downloads input CSV from S3 via Airflow S3Hook
│   ├── s3_uploader.py              # Uploads output files to S3 via Airflow S3Hook
│   └── save_chunks.py              # Saves DataFrame chunks as individual CSV files
│
├── utils/                          # Shared utilities
│   ├── __init__.py
│   ├── airflow_connection.py       # Fetches API URL from Airflow Connections
│   ├── chunker.py                  # Splits DataFrame into N-sized chunks
│   ├── dataframe_cleaner.py        # Removes duplicates and null rows
│   ├── error_handler.py            # Graceful API error handling + failure tracking
│   ├── logger.py                   # Centralized logger with timestamp formatting
│   └── metrics.py                  # Batch summary statistics
│
├── validators/
│   ├── __init__.py
│   └── vehicle_validator.py        # Schema validation (required columns check)
│
├── docker-compose.yaml             # Full Airflow cluster (7 services + Postgres + Redis)
├── .env.example                    # Template environment variables
├── .gitignore
└── README.md                       # ← You are here
```

---

## Step-by-Step Pipeline Workflow

The DAG `vehicle_batch_pipeline` executes 5 sequential/parallel tasks. Here is exactly what happens at each step:

---

### Step 1 → `load_vehicle_data()`

**Purpose:** Download, validate, and clean the raw vehicle CSV.

| # | Action | Detail |
|---|--------|--------|
| 1.1 | **Check Pipeline State** | Reads the Airflow Variable `PIPELINE_STATE`. If `PAUSED` or `CANCELLED`, the task immediately raises `AirflowFailException` and the entire DAG run stops. If `RUNNING` (default), continues. |
| 1.2 | **Download from S3** | Uses `S3Hook` (Airflow's Amazon provider) to download `s3://<S3_BUCKET>/input/vehicle_data.csv` to the local path `data/input/vehicle_data.csv`. The S3Hook authenticates via the Airflow Connection `aws_default`. |
| 1.3 | **Read CSV** | Loads the downloaded CSV into a Pandas DataFrame using `pd.read_csv()`. Logs the number of rows loaded. |
| 1.4 | **Validate Schema** | Checks that the DataFrame contains all required columns: `vehicle_id`, `brand`, `model`, `year`, `fuel_type`. If any column is missing, raises `ValueError` with the list of missing columns. |
| 1.5 | **Clean Data** | Removes duplicate rows (`drop_duplicates()`) and rows with any null values (`dropna()`). |
| 1.6 | **Return** | Converts the cleaned DataFrame to a list of dictionaries (`df.to_dict("records")`) and passes it to the next task via Airflow XCom. |

---

### Step 2 → `split_batches(records)`

**Purpose:** Split the cleaned records into smaller batch files for parallel processing.

| # | Action | Detail |
|---|--------|--------|
| 2.1 | **Reconstruct DataFrame** | Converts the list of dictionaries (received via XCom) back into a Pandas DataFrame. |
| 2.2 | **Get Chunk Size** | Reads the Airflow Variable `CHUNK_SIZE` (default = `2`). This controls how many vehicles are in each batch. |
| 2.3 | **Split into Chunks** | Uses `split_into_chunks(df, chunk_size)` which iterates through the DataFrame in steps of `chunk_size`, creating a list of smaller DataFrames. |
| 2.4 | **Save as CSV Files** | Each chunk is written to disk as `data/output/batch_1.csv`, `batch_2.csv`, etc. The `data/output/` directory is created automatically. |
| 2.5 | **Return** | Returns a list of batch file paths (strings) via XCom — this list is consumed by the next task's `expand()`. |

---

### Step 3 → `process_batch_task(batch_file)` × N (Dynamic Task Mapping)

**Purpose:** For each batch file, enrich every vehicle by calling 4 external APIs in parallel.

> ⚡ **This task uses Airflow's Dynamic Task Mapping** — `.expand(batch_file=batch_files)` creates one independent Celery task per batch file, all running in parallel.

| # | Action | Detail |
|---|--------|--------|
| 3.1 | **Read Batch File** | Loads the batch CSV into a DataFrame. |
| 3.2 | **Determine Environment** | Extracts the `environment` parameter from the DAG Run config (`dag_run.conf`). Defaults to `"staging"`. The environment maps to a base URL: staging → `http://host.docker.internal:8000`, production → same (configurable). |
| 3.3 | **Check Valuation Flag** | Reads the Airflow Variable `ENABLE_VALUATION` (default = `"true"`). If `"false"`, the Valuation API is skipped and returns `{"market_value": None, "valuation_status": "SKIPPED"}`. |
| 3.4 | **Enrich Each Vehicle** | For each row, calls `enrich_vehicle()` which uses a `ThreadPoolExecutor(max_workers=4)` to call all 4 APIs **concurrently**: |
|     | | → `POST /api/rc` — Returns `owner`, `registration_status` |
|     | | → `POST /api/fastag` — Returns `fastag_status`, `bank` |
|     | | → `POST /api/vrn` — Returns `blacklisted` (boolean) |
|     | | → `POST /api/valuation` — Returns `market_price` (if enabled) |
| 3.5 | **Retry Logic** | Each HTTP call is wrapped with `@retry` from Tenacity: **3 attempts**, **exponential backoff** (`1s, 2s, 4s`). On permanent failure, the error handler logs the failure and tracks it. |
| 3.6 | **Error Handling** | If any API call fails after all retries, `handle_api_error()` returns `{"status": "FAILED", "error": "<message>"}` and adds the record to the in-memory failed records list. The pipeline does NOT crash — it continues processing remaining vehicles. |
| 3.7 | **Batch Summary** | Logs a summary table showing total records, successes, failures, and success rate percentage. |
| 3.8 | **Failed Records** | If any vehicles failed, saves them to `data/failed/failed_batch_N.csv`. |
| 3.9 | **Return** | Returns the enriched DataFrame (via XCom) to be merged in the next step. |

**Task Configuration:**
- `retries = 2` (Airflow-level retries on top of Tenacity retries)
- `retry_delay = 30 seconds`
- `pool = "vehicle_api_pool"` (throttles concurrent API calls across workers)

---

### Step 4 → `merge_batches(results)`

**Purpose:** Combine all enriched batch DataFrames into a single final report and upload to S3.

| # | Action | Detail |
|---|--------|--------|
| 4.1 | **Concatenate** | Uses `pd.concat(results)` to merge all enriched DataFrames from every batch into one unified DataFrame. |
| 4.2 | **Save Report** | Writes the merged DataFrame to `data/output/final_vehicle_report.csv`. |
| 4.3 | **Upload Report to S3** | Uses `S3Hook.load_file()` to upload the report to `s3://<S3_BUCKET>/output/final_vehicle_report.csv` (replaces any existing file). |
| 4.4 | **Handle Failures** | Collects all failed records from the in-memory tracker. If any exist, saves them to `data/output/failed_records.csv` and uploads to `s3://<S3_BUCKET>/failed/failed_records.csv`. |
| 4.5 | **Return** | Returns the total row count for the completion task. |

---

### Step 5 → `pipeline_completed(total)`

**Purpose:** Final logging task — confirms pipeline completion and total rows processed.

| # | Action | Detail |
|---|--------|--------|
| 5.1 | **Log Completion** | Logs `"Pipeline completed. Rows=<N>"` to indicate successful end-to-end processing. |

---

### Full DAG Dependency Chain

```
load_vehicle_data  →  split_batches  →  process_batch_task (× N parallel)  →  merge_batches  →  pipeline_completed
```

---

## Setup — Local Development

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| **Docker Desktop** | Latest | Runs all containers |
| **Docker Compose** | v2+ | Orchestrates multi-container setup |
| **Git** | Any | Clone the repository |
| **Python** | 3.10+ | (Optional) Local script testing |

### Step 1: Clone the Repository

```bash
git clone https://github.com/<your-username>/vehicle-batch-orchestrator.git
cd vehicle-batch-orchestrator
```

### Step 2: Create the `.env` File

```bash
cp .env.example .env
```

Edit `.env` and set the values:

```env
# Airflow UID (run 'id -u' on Linux/Mac; use 50000 on Windows)
AIRFLOW_UID=50000

# Airflow admin credentials
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Additional pip packages installed at container startup
_PIP_ADDITIONAL_REQUIREMENTS=tenacity

# Generate a Fernet key:
#   python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
FERNET_KEY=<your-generated-fernet-key>

# JWT for Airflow API
AIRFLOW__API_AUTH__JWT_SECRET=change_me
AIRFLOW__API_AUTH__JWT_ISSUER=airflow
```

### Step 3: Start the Mock API Server

The mock API must be running **before** the Airflow pipeline executes:

```bash
cd mock-api
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

Or run it with Docker:

```bash
cd mock-api
docker build -t vehicle-mock-api .
docker run -d -p 8000:8000 --name vehicle-mock-api vehicle-mock-api
```

Verify it's running:

```bash
curl http://localhost:8000/docs    # Opens the FastAPI Swagger UI
```

### Step 4: Start the Airflow Cluster

```bash
# From the project root
docker compose up -d
```

This starts **9 services**:

| Service | Port | Description |
|---------|------|-------------|
| `postgres` | 5432 (internal) | Airflow metadata database |
| `redis` | 6379 (internal) | Celery message broker |
| `airflow-apiserver` | **8080** | Airflow Web UI & REST API |
| `airflow-scheduler` | — | Schedules and triggers DAGs |
| `airflow-dag-processor` | — | Parses DAG files |
| `airflow-worker` | — | Celery worker that executes tasks |
| `airflow-triggerer` | — | Handles deferred/async tasks |
| `airflow-init` | — | One-time DB migration + admin user creation |
| `airflow-cli` | — | Debug profile (optional) |

Wait ~60 seconds for all services to become healthy, then verify:

```bash
docker compose ps
```

### Step 5: Access the Airflow UI

Open **http://localhost:8080** in your browser.

- **Username:** `airflow` (or whatever you set in `.env`)
- **Password:** `airflow`

### Step 6: Configure Airflow Variables

Navigate to **Admin → Variables** in the Airflow UI and create:

| Key | Value | Description |
|-----|-------|-------------|
| `S3_BUCKET` | `vehicle-batch-airflow` | Your S3 bucket name |
| `CHUNK_SIZE` | `2` | Records per batch (increase for production) |
| `ENABLE_VALUATION` | `true` | Set `false` to skip valuation API |
| `PIPELINE_STATE` | `RUNNING` | Set `PAUSED` or `CANCELLED` to stop |

### Step 7: Configure the AWS Connection

Navigate to **Admin → Connections** and create:

| Field | Value |
|-------|-------|
| **Connection Id** | `aws_default` |
| **Connection Type** | Amazon Web Services |
| **AWS Access Key ID** | `<your-access-key>` |
| **AWS Secret Access Key** | `<your-secret-key>` |
| **Extra** | `{"region_name": "ap-south-1"}` |

> **Note:** For local testing without AWS, comment out the S3 download/upload calls in the DAG and use the sample CSV at `data/input/vehicle_data.csv` directly.

### Step 8: Trigger the Pipeline

1. Go to the **DAGs** page → find `vehicle_batch_pipeline`
2. Click the ▶️ **Trigger** button
3. (Optional) Pass a config JSON:
   ```json
   {
     "environment": "staging"
   }
   ```
4. Monitor execution in the **Graph** or **Grid** view

### Step 9: (Optional) Enable Celery Flower Monitoring

```bash
docker compose --profile flower up -d
```

Access Flower at **http://localhost:5555** to monitor Celery worker status and task queues.

### Stopping Everything

```bash
docker compose down           # Stop and remove containers
docker compose down -v        # Also remove volumes (⚠️ deletes Postgres data)
```

---

## Setup — AWS Deployment (EC2)

### Prerequisites

| Requirement | Detail |
|-------------|--------|
| **AWS Account** | With IAM user/role that has S3 access |
| **EC2 Instance** | Ubuntu 22.04+ (minimum `t2.medium` — 4GB RAM required) |
| **Security Groups** | Inbound: `22` (SSH), `8080` (Airflow UI), `8000` (Mock API), `5555` (Flower, optional) |
| **S3 Bucket** | e.g. `vehicle-batch-airflow` with folders: `input/`, `output/`, `failed/` |
| **SSH Key Pair** | Ed25519 key for GitHub Actions deployment |
| **Domain (optional)** | For production, point a domain to the EC2 public IP |

### Step 1: Launch and Connect to EC2

```bash
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>
```

### Step 2: Install Docker on EC2

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Install Docker
sudo apt install -y docker.io docker-compose-plugin

# Add ubuntu user to docker group
sudo usermod -aG docker ubuntu

# Logout and login again for group changes to take effect
exit
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>

# Verify Docker
docker --version
docker compose version
```

### Step 3: Clone the Repository

```bash
cd ~
git clone https://github.com/<your-username>/vehicle-batch-orchestrator.git
cd vehicle-batch-orchestrator
```

### Step 4: Create the `.env` File on EC2

```bash
cp .env.example .env
nano .env
```

Fill in all required values (same as local setup, but use **strong passwords** for production):

```env
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=<strong-password>
_PIP_ADDITIONAL_REQUIREMENTS=tenacity
FERNET_KEY=<generated-fernet-key>
AIRFLOW__API_AUTH__JWT_SECRET=<strong-random-secret>
AIRFLOW__API_AUTH__JWT_ISSUER=airflow
```

### Step 5: Start the Mock API on EC2

```bash
cd ~/vehicle-batch-orchestrator/mock-api
docker build -t vehicle-mock-api .
docker run -d -p 8000:8000 --name vehicle-mock-api vehicle-mock-api
```

### Step 6: Start the Airflow Cluster on EC2

```bash
cd ~/vehicle-batch-orchestrator
docker compose up -d
```

Wait ~60 seconds, then verify:

```bash
docker compose ps      # All services should show "healthy" or "running"
docker ps              # Mock API + Airflow services
```

### Step 7: Configure S3 Bucket

```bash
# Create the S3 bucket (if it doesn't exist)
aws s3 mb s3://vehicle-batch-airflow --region ap-south-1

# Upload the sample input file
aws s3 cp data/input/vehicle_data.csv s3://vehicle-batch-airflow/input/vehicle_data.csv
```

### Step 8: Configure Airflow on EC2

1. Open `http://<EC2_PUBLIC_IP>:8080`
2. Login with your admin credentials
3. Set up **Variables** and **AWS Connection** (same as local setup Step 6 & 7)

### Step 9: Set Up GitHub Actions for CI/CD

Add the following **secrets** in your GitHub repository (**Settings → Secrets → Actions**):

| Secret Name | Value |
|-------------|-------|
| `EC2_HOST` | Your EC2 public IP or hostname |
| `EC2_USER` | `ubuntu` |
| `EC2_SSH_KEY` | Contents of your private SSH key (`id_ed25519`) |

Now every push to `main` will automatically:
1. SSH into the EC2 instance
2. Stop running containers
3. Pull the latest code
4. Rebuild and restart all containers

### Step 10: Verify Deployment

```bash
# SSH into EC2 and check
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>
docker compose -f ~/vehicle-batch-orchestrator/docker-compose.yaml ps
```

---

## Airflow Variables & Configuration

These Airflow Variables control pipeline behavior at runtime without code changes:

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | `vehicle-batch-airflow` | S3 bucket for input/output files |
| `CHUNK_SIZE` | `2` | Number of vehicle records per batch (tune for performance) |
| `ENABLE_VALUATION` | `true` | Toggle valuation API calls (`true`/`false`) |
| `PIPELINE_STATE` | `RUNNING` | Control pipeline execution: `RUNNING`, `PAUSED`, or `CANCELLED` |

### S3 Key Paths (Hardcoded in `pipeline_config.py`)

| Path | Purpose |
|------|---------|
| `input/vehicle_data.csv` | Source input file |
| `output/final_vehicle_report.csv` | Enriched output report |
| `failed/failed_records.csv` | Records that failed API enrichment |

---

## Mock API Reference

The mock API simulates 4 external vehicle data services. All endpoints accept the same request schema.

### Request Body

```json
{
  "vehicle_id": "V001",
  "brand": "Toyota",
  "model": "Fortuner",
  "year": 2022,
  "fuel_type": "Diesel"
}
```

### Endpoints

| Method | Endpoint | Response Fields | Notes |
|--------|----------|-----------------|-------|
| `POST` | `/api/rc` | `vehicle_id`, `owner`, `registration_status` | ⚠️ **30% random failure rate** (simulates unreliable service) |
| `POST` | `/api/fastag` | `vehicle_id`, `fastag_status`, `bank` | Always returns `ACTIVE` / `HDFC` |
| `POST` | `/api/vrn` | `vehicle_id`, `blacklisted` | Always returns `false` |
| `POST` | `/api/valuation` | `vehicle_id`, `market_price` | Price = `max(₹3,00,000, ₹15,00,000 - age × ₹1,00,000)` |

### Running the Mock API

```bash
cd mock-api
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

Swagger docs available at: `http://localhost:8000/docs`

---

## CI/CD — GitHub Actions

The project includes a GitHub Actions workflow (`.github/workflows/deploy.yml`) that automatically deploys to an EC2 instance on every push to `main`.

### Deployment Flow

```
Push to main → GitHub Actions → SSH into EC2 → docker compose down →
git pull → docker compose up -d --build → Verify services
```

### What the Workflow Does

1. Checks out the repository
2. Configures SSH using repository secrets
3. SSHs into the EC2 instance
4. Stops all running containers (`docker compose down`)
5. Fixes file permissions
6. Pulls latest code (`git reset --hard origin/main`)
7. Rebuilds and starts containers (`docker compose up -d --build`)
8. Waits 20 seconds for services to stabilize
9. Prints the status of all containers

---

## Sample Data

The included sample CSV (`data/input/vehicle_data.csv`) contains:

| vehicle_id | brand | model | year | fuel_type |
|------------|-------|-------|------|-----------|
| V001 | Toyota | Fortuner | 2022 | Diesel |
| V002 | Hyundai | Creta | 2021 | Petrol |
| V003 | Tata | Nexon | 2023 | Electric |
| V004 | Mahindra | XUV700 | 2022 | Diesel |
| V005 | Honda | City | 2020 | Petrol |

After enrichment, each record will have additional fields: `owner`, `registration_status`, `fastag_status`, `bank`, `blacklisted`, and `market_price`.

---

## Troubleshooting

### Common Issues

| Problem | Solution |
|---------|----------|
| **Airflow UI not loading** | Wait 60s after `docker compose up`. Check `docker compose ps` for health status. |
| **`AIRFLOW_UID` warning** | Set `AIRFLOW_UID=50000` in `.env` (or run `id -u` on Linux). |
| **S3 download fails** | Verify `aws_default` connection in Airflow UI. Check IAM permissions for `s3:GetObject`. |
| **Mock API unreachable** | Ensure mock API is running on port 8000. Inside Docker, use `host.docker.internal:8000`. |
| **RC API failures** | Expected — the mock RC service has a 30% deliberate failure rate. Tenacity retries 3 times. |
| **Out of memory** | Docker needs **≥4GB RAM**. Increase Docker Desktop memory allocation. |
| **Tasks stuck in "queued"** | Check if Celery worker is running: `docker compose logs airflow-worker`. |
| **DAG not appearing** | Check `docker compose logs airflow-dag-processor` for import errors. |
| **Permission errors** | Run `sudo chown -R $(id -u):0 logs/ data/` on the host. |

### Useful Commands

```bash
# View real-time logs of a specific service
docker compose logs -f airflow-worker

# Restart a single service
docker compose restart airflow-scheduler

# Check Airflow health
curl http://localhost:8080/api/v2/monitor/health

# Enter the Airflow CLI container
docker compose run --rm airflow-cli bash

# List all Airflow DAGs
docker compose run --rm airflow-cli airflow dags list
```

---

## License

This project is provided as-is for educational and development purposes.