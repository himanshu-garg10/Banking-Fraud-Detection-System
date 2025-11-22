# **Banking Fraud Detection System**

### *End-to-End Big Data Pipeline using PySpark, FastAPI, and Streamlit*

---

## **Architecture Diagram**

```mermaid
flowchart TB

    subgraph SOURCE[Data Sources]
        A1[customers.csv]
        A2[transactions_json/]
        A3[compliance_notes.txt]
        A4[historical_fraud.parquet]
    end

    subgraph ETL[PySpark ETL Pipeline]
        B1[Read all raw files]
        B2[Data Cleaning & Normalization]
        B3[JSON Flattening]
        B4[Feature Engineering]
        B5[Risk Scoring Logic]
        B6[Write Outputs to /output]
    end

    subgraph OUT[/output/]
        C1[final/fused/]
        C2[alerts/]
        C3[aggregates/]
    end

    subgraph API[FastAPI Backend]
        D1[/]
        D2[/latest]
        D3[/latest_tx]
    end

    subgraph DASH[Streamlit Dashboard]
        E1[Overview Metrics]
        E2[Customer Drilldown]
        E3[High-Risk Alerts]
        E4[Live API Updates]
        E5[Data Explorer]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B1
    A4 --> B1

    B1 --> B2 --> B3 --> B4 --> B5 --> B6

    B6 --> C1
    B6 --> C2
    B6 --> C3

    C1 --> API
    C2 --> API
    API --> DASH
```

---

## **1. Project Overview**

This project is a complete real-world style **Fraud Detection Pipeline** that simulates how banks process transactions, calculate risk, generate alerts, store outputs, and build dashboards for internal investigation teams.

The system contains:

* **Automated ETL pipeline** built using **PySpark**
* **Multi-format data ingestion** (CSV, JSON, TXT, Parquet)
* **Feature engineering + risk scoring**
* **Dynamic high-risk alerts**
* **Customer-level statistical aggregates**
* **Backend API** built with **FastAPI**
* **Interactive Dashboard** built with **Streamlit**
* **Modular folder structure**, similar to a production environment

This is designed to look like a professional, industry-grade data engineering project suitable for resumes, GitHub portfolios, and academic submissions.

---

## **2. Features**

### **2.1 ETL Pipeline (PySpark)**

The ETL job performs:

* Reading multiple raw data sources:

  * `customers.csv`
  * `transactions_json/`
  * `compliance_notes.txt`
  * `historical_fraud.parquet`

* Cleaning and standardizing fields

* Extracting nested JSON fields

* Creating new engineered features:

  * device_mismatch
  * is_night
  * risk_score
  * risk_level
  * synthetic geo coordinates (lat, lon)

* Joining all data into one enriched dataset

* Writing final outputs in structured folders

---

### **2.2 Fraud Scoring Logic**

Each transaction is evaluated based on:

* Device mismatch
* Night-time usage
* Abnormal transaction amount
* Historical fraud pattern (optional)

Fraud score classification:

| Risk Score | Risk Level |
| ---------- | ---------- |
| 4 or more  | HIGH       |
| 2 to 4     | MEDIUM     |
| below 2    | LOW        |

High-risk rows are exported as **alerts**.

---

### **2.3 Backend API (FastAPI)**

Backend provides:

* `/` → health check
* `/latest` → test route
* `/latest_tx` → returns latest transactions to dashboard

The dashboard uses these for live updates.

---

### **2.4 Streamlit Dashboard**

A multi-page dashboard with:

* Overview metrics
* Risk distribution visualizations
* Top risky customers
* Live API metrics
* Live streaming transactions
* Customer drilldown
* High-risk alerts view
* Data explorer
* System status page

Charts include:

* Interactive histograms
* Line charts
* Bar charts
* Trend analysis

---

## **3. Project Structure**

```
big_data_project/
│
├── generate_demo_data.py          # synthetic data generator
├── etl_fraud_pipeline.py          # PySpark ETL pipeline
├── api_server.py                  # FastAPI backend
├── dashboard_fraud.py             # Streamlit dashboard
│
├── sample_data/
│   ├── customers.csv
│   ├── transactions_json/
│   ├── compliance_notes.txt
│   ├── historical_fraud.parquet
│   └── device_fingerprints.bin
│
├── output/
│   ├── final/
│   │   └── fused/                 # enriched dataset
│   ├── alerts/                    # high-risk alerts
│   └── aggregates/                # customer-level stats
│
└── venv/
```

---

## **4. How to Run the Entire System**

### **Step 1 – Activate Virtual Environment**

```
source venv/bin/activate
```

### **Step 2 – Generate Sample Data**

```
python generate_demo_data.py
```

### **Step 3 – Run ETL Pipeline**

```
python etl_fraud_pipeline.py
```

Outputs will appear under `/output`.

### **Step 4 – Start FastAPI Backend**

```
uvicorn api_server:app --reload
```

Open in browser:

```
http://127.0.0.1:8000
```

### **Step 5 – Launch Dashboard**

```
streamlit run dashboard_fraud.py
```

Dashboard URL:

```
http://localhost:8501
```

---

## **5. Technologies Used**

| Layer           | Technology               |
| --------------- | ------------------------ |
| Data Processing | Apache PySpark           |
| API Service     | FastAPI + Uvicorn        |
| Dashboard       | Streamlit                |
| Data Formats    | CSV, JSON, TXT, Parquet  |
| Language        | Python                   |
| Visualization   | Plotly, Streamlit Charts |

---

## **6. Use Cases**

This system can be used to simulate:

* Fraud monitoring in banks and fintechs
* Big data engineering pipelines
* PySpark data fusion + feature engineering
* Real-time dashboards for risk teams
* API–Dashboard integrated architectures

It is perfect for:

* College projects
* Portfolio/GitHub showcase
* Data engineering interview prep
* Learning Spark end-to-end workflows

---

## **7. Future Enhancements**

* Replace synthetic streaming with Kafka
* Add ML-based fraud prediction model
* Replace synthetic geo-coords with GeoIP lookup
* Add user authentication for dashboard
* Add historical trend storage in database (Snowflake/BigQuery/Postgres)

---


