-- ============================================================
-- ClickHouse Schema for Real-Time User Engagement Analytics
-- ============================================================

-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics;

-- ============================================================
-- Raw events table - stores all incoming user engagement events
-- Uses MergeTree engine with partitioning for fast queries
-- ============================================================
CREATE TABLE IF NOT EXISTS analytics.user_events
(
    event_id        String,
    user_id         String,
    content_id      String,
    action          LowCardinality(String),
    platform        LowCardinality(String),
    region          LowCardinality(String),
    session_id      String,
    timestamp       DateTime,
    duration_seconds Nullable(Int32),
    event_date      Date DEFAULT toDate(timestamp)
)
ENGINE = MergeTree()
PARTITION BY event_date
ORDER BY (user_id, timestamp)
SETTINGS index_granularity = 8192;

-- ============================================================
-- Engagement KPIs table - aggregated metrics per time window
-- ============================================================
CREATE TABLE IF NOT EXISTS analytics.engagement_kpis
(
    window_start    DateTime,
    window_end      DateTime,
    action          LowCardinality(String),
    region          LowCardinality(String),
    event_count     UInt64,
    unique_users    UInt64,
    unique_content  UInt64
)
ENGINE = MergeTree()
ORDER BY (window_start, action, region)
SETTINGS index_granularity = 8192;

-- ============================================================
-- Materialized view for real-time hourly aggregations
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.hourly_engagement
ENGINE = SummingMergeTree()
ORDER BY (hour, action, region)
AS SELECT
    toStartOfHour(timestamp)   AS hour,
    action,
    region,
    count()                     AS event_count,
    uniq(user_id)               AS unique_users,
    uniq(content_id)            AS unique_content
FROM analytics.user_events
GROUP BY hour, action, region;
