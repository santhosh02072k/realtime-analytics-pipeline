# Real-Time User Engagement Analytics Pipeline

![Stack](https://img.shields.io/badge/Stack-Kafka%20%2B%20Flink%20%2B%20ClickHouse-blue)
![Status](https://img.shields.io/badge/Status-In%20Progress-yellow)
![License](https://img.shields.io/badge/License-MIT-green)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue)

## Overview

A production-grade real-time streaming pipeline that simulates a content platform event stream (user views, likes, shares, comments) — similar to how platforms like TikTok or YouTube process user engagement data at scale.

Built to demonstrate end-to-end ownership of a distributed data system: from event ingestion through stream processing to analytical querying.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Event Producer (Python)                │
│         Simulates user engagement events at scale        │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│                   Apache Kafka                           │
│   High-throughput partitioned log — event bus            │
│   Topic: user_events (partitioned by user_id)            │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│                   Apache Flink                           │
│   Stateful stream processing with event-time windowing   │
│   Exactly-once processing semantics                      │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│                   ClickHouse                             │
│   Columnar OLAP storage — MergeTree engine               │
│   Sub-second query performance on 1M+ events             │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│                   SQL Reporting Layer                    │
│   Real-time KPI dashboards for cross-functional teams    │
└─────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology | Why Chosen |
|---|---|---|
| Event Bus | Apache Kafka | High-throughput partitioned log architecture |
| Stream Processing | Apache Flink | Stateful processing with event-time windowing |
| OLAP Storage | ClickHouse | Columnar storage, 10x faster than row-based DBs |
| Producer | Python | Simulates realistic user engagement events |
| Infrastructure | Docker Compose | Single command setup |

---

## Project Structure

```
realtime-analytics-pipeline/
├── docker-compose.yml              # Spins up Kafka, Flink, ClickHouse
├── requirements.txt                # Python dependencies
├── README.md
│
├── producer/
│   └── event_producer.py          # Simulates user events to Kafka
│
├── flink_jobs/
│   └── stream_processor.py        # Flink job: Kafka to ClickHouse
│
├── clickhouse/
│   └── schema.sql                 # MergeTree tables + materialized views
│
└── queries/
    └── analytics_queries.sql      # KPI reporting queries
```

---

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- Python 3.8+
- Git

### 1. Clone the repository
```bash
git clone https://github.com/santhosh02072k/realtime-analytics-pipeline.git
cd realtime-analytics-pipeline
```

### 2. Start all services
```bash
docker-compose up -d
```

Verify services are running:
```bash
docker-compose ps
```

### 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 4. Initialize ClickHouse schema
```bash
docker exec -i clickhouse clickhouse-client < clickhouse/schema.sql
```

### 5. Start the event producer
```bash
python producer/event_producer.py
```

### 6. Run the Flink stream processor
```bash
python flink_jobs/stream_processor.py
```

### 7. Query your data
```bash
docker exec -i clickhouse clickhouse-client --query "
SELECT action, count() AS events
FROM analytics.user_events
GROUP BY action
ORDER BY events DESC"
```

---

## Sample Analytics Queries

See `queries/analytics_queries.sql` for the full set. Examples:

**Top content by engagement rate:**
```sql
SELECT content_id,
       countIf(action = 'view')  AS views,
       countIf(action = 'like')  AS likes,
       round(countIf(action = 'like') * 100.0
             / nullIf(countIf(action = 'view'), 0), 2) AS like_rate_pct
FROM analytics.user_events
WHERE event_date = today()
GROUP BY content_id
ORDER BY like_rate_pct DESC
LIMIT 10;
```

**Unique active users per hour:**
```sql
SELECT toStartOfHour(timestamp) AS hour,
       uniq(user_id)            AS unique_users
FROM analytics.user_events
WHERE timestamp >= now() - INTERVAL 12 HOUR
GROUP BY hour
ORDER BY hour DESC;
```

---

## Key Design Decisions

- **Kafka over RabbitMQ**: Kafka's partitioned log architecture provides better throughput and replay capability for high-velocity event streams
- **Flink over Spark Streaming**: Flink's native event-time processing and exactly-once semantics make it better suited for real-time use cases
- **ClickHouse over PostgreSQL**: Columnar storage and MergeTree engine deliver sub-second OLAP performance on millions of events where row-based databases would be 10x slower

---

## Author

**Santhosh Ekambaram**
M.S. Computer Science, Illinois Institute of Technology, Chicago
AWS Certified Solutions Architect

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin)](https://linkedin.com/in/santhosh-ekambaram-1457241a2)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=flat&logo=github)](https://github.com/santhosh02072k)
