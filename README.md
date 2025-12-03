# 🛡️ Ecommerce Fraud Detection Pipeline

![Python](https://img.shields.io/badge/Python-3.9-blue?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.6.3-017CEE?logo=apacheairflow&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?logo=apachekafka&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-BigQuery-FF694B?logo=dbt&logoColor=white)
![BigQuery](https://img.shields.io/badge/Google%20BigQuery-Data%20Warehouse-4285F4?logo=googlecloud&logoColor=white)

## 📖 Overview

**Ecommerce Fraud Detection Pipeline** adalah solusi *End-to-End Data Engineering* yang menggabungkan pemrosesan **Real-time Streaming** dan **Batch ETL** untuk mendeteksi transaksi curang (fraud) pada platform e-commerce.

Project ini mensimulasikan data transaksi, melakukan deteksi anomali secara *real-time*, mengirimkan notifikasi peringatan (alerts), dan melakukan transformasi data analitik untuk kebutuhan pelaporan bisnis.

### 🎯 Key Features
* **Real-time Fraud Detection:** Mendeteksi pola mencurigakan (transaksi tengah malam, lokasi asing, penyalahgunaan voucher) menggunakan **Apache Kafka** dan **Python Consumers**.
* **Instant Alerting:** Mengirim notifikasi otomatis ke **Discord** saat fraud terdeteksi.
* **Hybrid Ingestion:** * *Streaming*: Order transactions via Kafka.
    * *Batch*: User, Product, & Voucher data via **Apache Airflow** (Hourly).
* **Data Warehouse & Modeling:** ETL harian dari PostgreSQL ke **Google BigQuery**, ditransformasi menggunakan **DBT** (Data Build Tool) menjadi Star Schema.
* **Containerized Environment:** Seluruh infrastruktur berjalan di atas **Docker**.

---
## 📂 Repository Structure
Struktur direktori diatur secara modular untuk memudahkan maintenance dan deployment.
```
├── dags/                           # Airflow Directed Acyclic Graphs
│   ├── init_schema_dag.py          # Inisialisasi tabel PostgreSQL
│   ├── ingest_products_dag.py      # Batch ingestion: Products
│   ├── ingest_users_dag.py         # Batch ingestion: Users
│   ├── ingest_discounts_dag.py     # Batch ingestion: Vouchers
│   ├── ingest_to_bigquery_dag.py   # ETL Pipeline (Postgres -> BigQuery)
│   └── dbt_run_dag.py              # Trigger DBT Job
├── fraud_analytics/                # dbt Project Directory
│   ├── models/
│   │   ├── staging/                # View materialization dari raw data
│   │   ├── core/                   # Fact & Dimension tables
│   │   └── marts/                  # Aggregated tables for BI
│   ├── profiles.yml                # Konfigurasi koneksi BigQuery
│   └── dbt_project.yml
├── scripts/
│   └── data_generator.py           # Faker logic untuk generate dummy data
├── streaming/
│   ├── producer.py                 # Kafka Producer (Simulasi Transaksi)
│   └── consumer.py                 # Kafka Consumer (Fraud Detection Logic)
├── docker-compose.yml              # Definisi Container Services
└── requirements.txt                # Dependensi Python
```
---
## 🏗️ Architecture

Sistem ini terdiri dari beberapa komponen utama:

1.  **Data Generator (Producer):** Script Python yang mensimulasikan transaksi user menggunakan `Faker`.
2.  **Streaming Layer:** **Apache Kafka** & Zookeeper menangani antrian data transaksi berkecepatan tinggi.
3.  **Processing Layer:** * *Consumer:* Membaca data Kafka, mengevaluasi *Fraud Rules*, dan menyimpan ke DB.
    * *Airflow:* Mengorkestrasi ingest data master dan pemindahan data ke BigQuery.
4.  **Storage Layer:** * **PostgreSQL:** Operational Database (OLTP).
    * **Google BigQuery:** Analytical Data Warehouse (OLAP).
5.  **Analytics Layer:** **DBT** mengubah data mentah menjadi Marts siap pakai untuk visualisasi.

---

## 🕵️‍♂️ Fraud Detection Logic

Sistem secara otomatis menandai transaksi sebagai **FRAUD** jika memenuhi kriteria berikut:

| Rule Name | Kondisi |
| :--- | :--- |
| **Foreign Location** | Transaksi berasal dari luar negara (Country != 'ID'). |
| **High Qty at Night** | Jumlah barang > 100 unit & Transaksi dilakukan pukul 00:00 - 04:00. |
| **High Amount at Night** | Total harga > Rp 100 Juta & Transaksi dilakukan pukul 00:00 - 04:00. |
| **Voucher Abuse** | Menggunakan kode voucher pada transaksi kecil (< Rp 50.000). |

---

## 🛠️ Tech Stack

* **Language:** Python 3.9
* **Orchestration:** Apache Airflow 2.6.3
* **Message Broker:** Apache Kafka & Zookeeper
* **Database:** PostgreSQL 15
* **Data Warehouse:** Google BigQuery
* **Transformation:** DBT (Data Build Tool)
* **Infrastructure:** Docker & Docker Compose
* **Alerting:** Discord Webhook

---

## 🚀 Getting Started

### Prerequisites
* Docker & Docker Compose terinstall.
* Akun Google Cloud Platform (GCP) dengan Service Account Key.
* Discord Webhook URL (Opsional untuk alert).

### Installation Steps

1.  **Clone Repository**
    ```bash
    git clone [https://github.com/username/ecommerce-fraud-detection.git](https://github.com/username/ecommerce-fraud-detection.git)
    cd ecommerce-fraud-detection
    ```

2.  **Setup Credentials**
    * Letakkan file Service Account GCP Anda di folder `keys/` dengan nama `gcp_key.json`.
    * Buat file `.env` (atau set environment variable) untuk konfigurasi database dan webhook (lihat `docker-compose.yml` untuk referensi).

3.  **Start Infrastructure**
    Jalankan seluruh layanan menggunakan Docker Compose:
    ```bash
    docker-compose up -d
    ```
    *Tunggu beberapa saat hingga container sehat (healthy).*

4.  **Access Services**
    * **Airflow UI:** `http://localhost:8080` (User: `admin`, Pass: `admin`).
    * **PostgreSQL:** `localhost:5432` (Project DB) & `5434` (Airflow DB).

5.  **Run Pipeline**
    * Aktifkan DAG `0_init_schema` di Airflow untuk membuat tabel.
    * Jalankan script Producer untuk mulai mengirim data dummy:
        ```bash
        docker exec -it airflow-scheduler python /opt/airflow/streaming/producer.py
        ```
    * Jalankan Consumer untuk memproses data:
        ```bash
        docker exec -it airflow-scheduler python /opt/airflow/streaming/consumer.py
        ```
 6. **Run Batch Processing**
    * 1_ingest_users, 1_ingest_products, 1_ingest_discounts (Generate Master Data).
    * 2_ingest_to_bigquery (Load data harian ke BigQuery).
    * 3_dbt_fraud_analytics (Jalankan transformasi data).
---

## 📊 Data Models (DBT)

Pipeline ini menghasilkan tabel analitik (Marts) di BigQuery untuk menjawab pertanyaan bisnis:

* **`mart_total_saved_grand`**: KPI utama yang menghitung total uang yang berhasil diselamatkan dari upaya penipuan.
* **`mart_fraud_by_product`**: Analisis kategori produk mana yang paling sering menjadi target penipuan.
* **`mart_fraud_analysis`**: Profiling user yang melakukan fraud beserta lokasinya.

---

## 👤 Author

**Zidan**
* [LinkedIn](https://linkedin.com/in/zidanalfarizi) | [GitHub](https://github.com/zidanlf)

---
*Project ini dibuat sebagai bagian dari Final Project Data Engineering.*
