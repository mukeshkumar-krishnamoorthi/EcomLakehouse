# 🛍️ E-Commerce Data Lakehouse (EcomLakehouse)

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Apache Airflow 3.3.2](https://img.shields.io/badge/Airflow-3.3.2-teal.svg)](https://airflow.apache.org/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![MinIO Object Storage](https://img.shields.io/badge/MinIO-S3_Compatible-red.svg)](https://min.io/)
[![Package Manager uv](https://img.shields.io/badge/uv-managed-purple.svg)](https://astral.sh/uv)

A modern, production-grade **E-Commerce Data Lakehouse Architecture** built with Python, Apache Airflow, PostgreSQL, and S3-compatible Object Storage (MinIO). The platform ingests transactional data using high-throughput batch and incremental watermark-driven pipelines into a columnar **Bronze Parquet Storage Layer**, powered by automated data generators and scheduled orchestration workflows.

---

## 📋 Table of Contents

- [Architectural Overview](#-architectural-overview)
- [Medallion Lakehouse Architecture](#-medallion-lakehouse-architecture)
- [Source Data Model & ERD](#-source-data-model--erd)
- [Key Components & Modules](#-key-components--modules)
- [Technology Stack](#-technology-stack)
- [Repository Structure](#-repository-structure)
- [Configuration & Environment Variables](#-configuration--environment-variables)
- [Prerequisites](#-prerequisites)
- [Step-by-Step Setup & Getting Started](#-step-by-step-setup--getting-started)
- [Airflow Orchestration & DAGs](#-airflow-orchestration--dags)
- [Data Ingestion Engine & Storage Format](#-data-ingestion-engine--storage-format)
- [Synthetic Data & Incremental Simulator](#-synthetic-data--incremental-simulator)
- [Testing & Verification](#-testing--verification)
- [Future Roadmap](#-future-roadmap)

---

## 🏗️ Architectural Overview

The **EcomLakehouse** architecture extracts raw transactional entity records from an operational PostgreSQL database and lands them partitioned by date in an S3-compatible object store as optimized Parquet files.

```mermaid
flowchart TD
    subgraph Operational["Operational Source System"]
        DB[("PostgreSQL 16<br/>ecommerce DB")]
        SEED["Seed Data Generator<br/>seed_source.py"]
        INC_SEED["Live Activity Simulator<br/>incremental_seed_generator.py"]
    end

    subgraph Airflow["Airflow 3.3.2 Orchestration Cluster"]
        SCHED["Airflow Scheduler"]
        APISVR["Airflow API Server"]
        PROC["DAG Processor"]
        META[("Airflow Postgres Metadata")]
    end

    subgraph Ingestion["Ingestion Pipeline Core"]
        BATCH_INGEST["Full Batch Extractor<br/>ingestion/batch.py"]
        INC_INGEST["Incremental Extractor<br/>ingestion/incremental.py"]
        WRITER["Parquet Bronze Writer<br/>bronze/writer.py"]
    end

    subgraph Storage["Storage Layer"]
        MINIO["MinIO / AWS S3<br/>Bucket: ecommerce-lake"]
        BRONZE["Bronze Layer<br/>s3://ecommerce-lake/bronze/{table}/..."]
    end

    SEED -->|"Initial 10k orders / 1k customers"| DB
    INC_SEED -->|"Simulates live orders, updates & payments"| DB

    SCHED -->|"Trigger Every 10 mins"| INC_SEED
    SCHED -->|"Trigger Every 30 mins"| INC_INGEST
    SCHED -->|"Manual / Batch Execution"| BATCH_INGEST

    DB --> BATCH_INGEST
    DB --> INC_INGEST

    BATCH_INGEST --> WRITER
    INC_INGEST --> WRITER

    WRITER -->|"Write Partitioned Parquet"| BRONZE
```

---

## 🥇 Medallion Lakehouse Architecture

The lakehouse adopts the standard **Medallion Pattern** for data management:

| Layer      | Path / Target                                                   | Storage Format             | Description                                                                                                       | Status     |
| :--------- | :-------------------------------------------------------------- | :------------------------- | :---------------------------------------------------------------------------------------------------------------- | :--------- |
| **Bronze** | `s3://ecommerce-lake/bronze/{table}/ingestion_date=YYYY-MM-DD/` | Apache Parquet             | Append-only raw data extracted directly from source tables, immutable, append partitioned by ingestion timestamp. | **Active** |
| **Silver** | `s3://ecommerce-lake/silver/`                                   | Delta / Iceberg / Parquet  | Cleansed, deduplicated, CDC updated, schema-enforced entity tables.                                               | _Planned_  |
| **Gold**   | `s3://ecommerce-lake/gold/`                                     | Dimensional Models / Marts | Star-schema analytics, KPI aggregates (Revenue, Retention, Sales Funnels).                                        | _Planned_  |

---

## 🗄️ Source Data Model & ERD

The transactional engine tracks customers, products, categories, stores, orders, order line items, and payment transactions:

```mermaid
erDiagram
    categories ||--o{ products : contains
    products ||--o{ order_items : ordered_in
    customers ||--o{ orders : places
    stores ||--o{ orders : fulfills
    orders ||--o{ order_items : includes
    orders ||--o{ payments : paid_by
    payment_methods ||--o{ payments : used_in

    categories {
        bigserial category_id PK
        varchar category_name
        timestamp created_at
        timestamp updated_at
    }

    products {
        bigserial product_id PK
        bigint category_id FK
        varchar product_name
        varchar sku
        numeric price
        integer stock_quantity
        boolean is_active
        timestamp created_at
        timestamp updated_at
    }

    customers {
        bigserial customer_id PK
        varchar first_name
        varchar last_name
        varchar email
        varchar phone
        varchar city
        varchar state
        varchar country
        timestamp created_at
        timestamp updated_at
    }

    stores {
        bigserial store_id PK
        varchar store_name
        varchar city
        varchar state
        timestamp created_at
        timestamp updated_at
    }

    payment_methods {
        bigserial payment_method_id PK
        varchar method_name
        timestamp created_at
    }

    orders {
        bigserial order_id PK
        bigint customer_id FK
        bigint store_id FK
        varchar order_status
        timestamp order_date
        numeric total_amount
        timestamp created_at
        timestamp updated_at
    }

    order_items {
        bigserial order_item_id PK
        bigint order_id FK
        bigint product_id FK
        integer quantity
        numeric unit_price
        timestamp created_at
        timestamp updated_at
    }

    payments {
        bigserial payment_id PK
        bigint order_id FK
        bigint payment_method_id FK
        varchar payment_status
        numeric amount
        varchar transaction_reference
        timestamp paid_at
        timestamp created_at
        timestamp updated_at
    }
```

---

## 🧩 Key Components & Modules

### 1. Ingestion Engine (`src/ecomlakehouse/ingestion/`)

- **`postgres.py`**: Handles SQLAlchemy connection pooling and executes optimized pandas SQL queries for batch and timestamp-based incremental extracts.
- **`batch.py`**: Executes full snapshot extractions across all 8 transactional tables, optionally wiping prior raw landed objects in Bronze before reloading.
- **`incremental.py`**: Leverages Airflow execution interval watermarks (`prev_data_interval_end_success` to `data_interval_end`) to fetch only newly inserted or modified records (`updated_at` filter).

### 2. Bronze Writer (`src/ecomlakehouse/bronze/writer.py`)

- Transforms pandas DataFrames into compressed Parquet buffers via PyArrow.
- Streams Parquet files to MinIO/S3 using the following object key structure:
  ```text
  bronze/{table_name}/ingestion_date={YYYY-MM-DD}/{table_name}_{YYYYMMDDTHHMMSSZ}.parquet
  ```

### 3. Data Generator Engine (`src/ecomlakehouse/generator/`)

- **`seed_source.py`**: Performs initial bootstrapping of master data (categories, stores, payment methods), 1,000 customers, 100 products, and 10,000 historic orders.
- **`incremental_seed_generator.py`**: Simulates production e-commerce activity:
  - Registers 5–15 new customers per cycle.
  - Updates 1–5 customer profiles.
  - Generates 10–25 new multi-item orders calculated using exact catalog prices, discounts (30% odds), 18% tax, and conditional shipping fees.
  - Simulates payment authorizations (90% success rate).
  - Drives state machine updates for active orders (`PENDING` $\rightarrow$ `CONFIRMED` $\rightarrow$ `PROCESSING` $\rightarrow$ `SHIPPED` $\rightarrow$ `DELIVERED`).

### 4. Storage Utility (`src/ecomlakehouse/utils/storage.py`)

- Centralized Boto3 S3 client creator configured for MinIO or AWS S3 endpoint authentication, S3v4 signature protocols, and bucket configuration.

---

## 🛠️ Technology Stack

- **Core Language**: Python 3.11+
- **Dependency Management**: [`uv`](https://astral.sh/uv) & `pyproject.toml`
- **Orchestration**: Apache Airflow 3.3.2 (API Server, Scheduler, DAG Processor, LocalExecutor)
- **Primary Relational DB**: PostgreSQL 16
- **Object Storage**: MinIO (S3 API Compatible)
- **Data Libraries**: Pandas, PyArrow, SQLAlchemy, Psycopg 3, Boto3, Pendulum, Faker, Pydantic

---

## 📁 Repository Structure

```text
EcomLakehouse/
├── .env.example                     # Environment template file
├── .gitignore                       # Git ignore rules (logs, venv, data)
├── pyproject.toml                   # UV Project dependencies & build configuration
├── uv.lock                          # Deterministic dependency lockfile
├── docker-compose.yml               # Root services (PostgreSQL source + MinIO Object Store)
├── README.md                        # Project documentation
│
├── airflow/                         # Airflow Environment & DAGs
│   ├── docker-compose.yml           # Airflow 3.3.2 stack (API, Scheduler, Postgres, Processor)
│   └── dags/                        # Active Airflow Pipelines
│       ├── ecom_initial_bronze_ingestion.py   # Full batch initial ingestion DAG
│       ├── incremental_ingestion.py           # 30-min incremental watermark DAG
│       ├── incremental_seed_generator.py     # 10-min live synthetic traffic DAG
│       └── test_ecom_postgres.py              # PostgreSQL connectivity test DAG
│
├── config/                          # Configuration files directory
├── sql/
│   └── 01_source_schema.sql         # Source DDL for PostgreSQL database
│
└── src/
    └── ecomlakehouse/               # Core Python Package (`ecomlakehouse`)
        ├── bronze/
        │   └── writer.py            # Converts DataFrames & pushes Parquet to S3/MinIO
        ├── generator/
        │   ├── seed_source.py       # Initial source database seeder
        │   └── incremental_seed_generator.py  # Realistic transaction simulator
        ├── ingestion/
        │   ├── batch.py             # Full snapshot batch extraction logic
        │   ├── incremental.py       # Watermark incremental extraction logic
        │   └── postgres.py          # SQLAlchemy PostgreSQL query helper
        ├── quality/                 # (Placeholder) Data quality assertions
        ├── silver/                  # (Placeholder) Silver cleaning & transformations
        ├── gold/                    # (Placeholder) Gold dimensional modeling
        └── utils/
            ├── storage.py           # S3 / MinIO client initializer
            └── test_storage.py      # Storage connectivity helper test script
```

---

## ⚙️ Configuration & Environment Variables

Copy `.env.example` to `.env` in the root directory before launching services:

```ini
# S3 / MinIO Object Storage
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin123
S3_BUCKET=ecommerce-lake
S3_REGION=us-east-1

# Source PostgreSQL Database Connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5441
POSTGRES_USER=ecom_user
POSTGRES_PASSWORD=ecom_password
POSTGRES_DB=ecommerce

# Synthetic Generator Parameters (Optional Overrides)
NEW_CUSTOMERS_MIN=5
NEW_CUSTOMERS_MAX=15
NEW_ORDERS_MIN=10
NEW_ORDERS_MAX=25
```

> [!NOTE]
> Inside the Docker container network (`ecom-lakehouse-network`), Airflow services connect to PostgreSQL via host `ecom-postgres:5432` and MinIO via endpoint `http://ecom-minio:9000`.

---

## 🖥️ Port Matrix & Services Overview

| Container Service          | Exposed Host Port | Container Port | Service Description             | Credentials                    |
| :------------------------- | :---------------- | :------------- | :------------------------------ | :----------------------------- |
| **`ecom-postgres`**        | `5441`            | `5432`         | Source E-Commerce PostgreSQL DB | `ecom_user` / `ecom_password`  |
| **`ecom-minio` (API)**     | `9000`            | `9000`         | S3 API Endpoint                 | `minioadmin` / `minioadmin123` |
| **`ecom-minio` (Console)** | `9001`            | `9001`         | MinIO Web Console UI            | `minioadmin` / `minioadmin123` |
| **`airflow-apiserver`**    | `8080`            | `8080`         | Airflow Web UI & API            | `airflow` / `airflow`          |
| **`airflow-postgres`**     | `5440`            | `5432`         | Airflow Metadata Storage DB     | `airflow` / `airflow`          |

---

## ⚡ Prerequisites

Ensure the following tools are installed on your host environment:

1. [Docker](https://www.docker.com/) & Docker Compose (v2.20+)
2. [Python 3.11+](https://www.python.org/)
3. [`uv`](https://astral.sh/uv) package manager
4. `git` & `curl`

---

## 🚀 Step-by-Step Setup & Getting Started

### Step 1: Clone Repository & Setup Virtual Environment

```bash
git clone https://github.com/mukeshkumar-krishnamoorthi/EcomLakehouse.git
cd EcomLakehouse

# Sync dependencies using uv
uv sync
```

### Step 2: Configure Environment Files

```bash
cp .env.example .env
```

### Step 3: Create Docker Network

Create the shared external bridge network required for cross-container communication:

```bash
docker network create ecom-lakehouse-network
```

### Step 4: Start Operational Services (PostgreSQL & MinIO)

```bash
docker compose up -d
```

Verify container health:

```bash
docker compose ps
```

### Step 5: Initialize Source Schema & Seed Initial Transactional Data

Execute the DDL schema file and populate base historical records:

```bash
# Apply schema tables
uv run python -c "
from sqlalchemy import create_engine, text
engine = create_engine('postgresql+psycopg://ecom_user:ecom_password@localhost:5441/ecommerce')
with open('sql/01_source_schema.sql') as f:
    with engine.begin() as conn:
        conn.execute(text(f.read()))
print('Schema created successfully!')
"

# Seed base historical source data (10k orders, 1k customers)
uv run python src/ecomlakehouse/generator/seed_source.py
```

### Step 6: Create MinIO Storage Bucket

Access MinIO Console at [`http://localhost:9001`](http://localhost:9001) or create bucket via CLI / Python script:

```bash
uv run python -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3', endpoint_url='http://localhost:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin123', config=Config(signature_version='s3v4'))
s3.create_bucket(Bucket='ecommerce-lake')
print('Bucket ecommerce-lake ready!')
"
```

### Step 7: Launch Apache Airflow Stack

```bash
docker compose -f airflow/docker-compose.yml up -d
```

Check Airflow initialization logs:

```bash
docker compose -f airflow/docker-compose.yml logs -f airflow-init
```

Once initialized, open the Airflow UI at [`http://localhost:8080`](http://localhost:8080) (User: `airflow`, Password: `airflow`).

### Step 8: Configure Airflow PostgreSQL Connection

In the Airflow Web UI:

1. Navigate to **Admin** $\rightarrow$ **Connections**.
2. Add a new Connection:
   - **Connection Id**: `ecom_postgres`
   - **Connection Type**: `Postgres`
   - **Host**: `ecom-postgres`
   - **Database**: `ecommerce`
   - **Login**: `ecom_user`
   - **Password**: `ecom_password`
   - **Port**: `5432`
3. Click **Test** and **Save**.

---

## 🔄 Airflow Orchestration & DAGs

| DAG ID                          | Schedule                   | Catchup | Description                                                                                                                          |
| :------------------------------ | :------------------------- | :------ | :----------------------------------------------------------------------------------------------------------------------------------- |
| `ecom_initial_bronze_ingestion` | Manual (`None`)            | `False` | Performs full snapshot batch extractions for all source tables and writes Parquet objects to Bronze S3 storage.                      |
| `incremental_ingestion`         | `0/30 * * * *` (Every 30m) | `False` | Watermark-driven extract fetching records where `updated_at` falls between `prev_data_interval_end_success` and `data_interval_end`. |
| `incremental_seed_generator`    | `*/10 * * * *` (Every 10m) | `False` | Simulates real-time e-commerce user activity (new user signups, order placements, payments, updates).                                |
| `test_ecom_postgres`            | Manual                     | `False` | Verification DAG to test PostgreSQL hook connectivity inside Airflow tasks.                                                          |

---

## 💾 Data Ingestion Engine & Storage Format

Data landed in the Bronze storage layer is partitioned by ingestion date:

```text
ecommerce-lake/
└── bronze/
    ├── categories/
    │   └── ingestion_date=2026-10-08/
    │       └── categories_20261008T163000Z.parquet
    ├── customers/
    │   └── ingestion_date=2026-10-08/
    │       └── customers_20261008T163000Z.parquet
    ├── orders/
    │   └── ingestion_date=2026-10-08/
    │       └── orders_20261008T163000Z.parquet
    ├── order_items/
    ├── payment_methods/
    ├── payments/
    ├── products/
    └── stores/
```

---

## 🧪 Testing & Verification

Run tests to verify package integrity and storage utilities:

```bash
# Run pytest suite
uv run pytest

# Verify S3 connection
uv run python src/ecomlakehouse/utils/test_storage.py
```

---

## 🛣️ Future Roadmap

- [ ] **Silver Layer (dbt & Apache Spark)**: Implement CDC (Change Data Capture) merges, data deduplication, schema enforcement, and data modeling using dbt-core and PySpark.
- [ ] **Data Quality Gateways**: Integrate Great Expectations / Soda SQL assertions on landed Bronze & Silver datasets.
- [ ] **Gold Layer Dimensional Models**: Build analytics-ready Kimball star schemas (Fact Sales, Dim Customers, Dim Products).
- [ ] **Interactive Query Engine**: Add Trino / Presto service container for SQL analytics over S3 Parquet tables.

---

## 👤 Author & License

Developed by **Mukeshkumar Krishnamoorthi** ([mukeshkumar.krishnamoorthi@gmail.com](mailto:mukeshkumar.krishnamoorthi@gmail.com)).
Distributed under the MIT License.
