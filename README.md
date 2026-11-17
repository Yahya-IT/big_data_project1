# 🚦 Real-Time Traffic Accidents Big Data Pipeline

An end-to-end real-time streaming data pipeline that ingests traffic accident records, cleans and validates events in real time, performs micro-batch aggregations with Upsert persistence, and visualizes live KPIs on an interactive web dashboard.

---

## 🏗️ System Architecture

```text
[CSV Dataset]
     │
     ▼
[Python Kafka Producer] ──► Kafka Topic: `raw-accidents`
                                    │
                                    ▼
                          [Apache NiFi Flow]
                     (Schema Validation & Cleaning)
                                    │
                                    ▼
                        Kafka Topic: `cleaned-accidents`
                                    │
                                    ▼
                   [Spark Structured Streaming]
                  (5s Micro-Batch Aggregations)
                                    │
                                    ▼ (PostgreSQL Upsert)
                     [PostgreSQL Data Warehouse]
                      ├── geo_accidents
                      ├── time_accidents
                      ├── weather_road_accidents
                      ├── factor_accidents
                      └── vehicle_accidents
                                    │
                                    ▼ (Live SQL Polling)
                      [Streamlit Live Dashboard]
