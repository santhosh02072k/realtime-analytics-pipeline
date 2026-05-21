-- ============================================================
-- Analytics Queries for Real-Time User Engagement Dashboard
-- Run these against ClickHouse analytics database
-- ============================================================

-- ------------------------------------------------------------
-- 1. Total events by action type (last 24 hours)
-- ------------------------------------------------------------
SELECT
    action,
    count()                                     AS total_events,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS percentage
FROM analytics.user_events
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY action
ORDER BY total_events DESC;


-- ------------------------------------------------------------
-- 2. Top 10 most viewed content pieces
-- ------------------------------------------------------------
SELECT
    content_id,
    countIf(action = 'view')    AS views,
    countIf(action = 'like')    AS likes,
    countIf(action = 'share')   AS shares,
    countIf(action = 'comment') AS comments,
    round(countIf(action = 'like') * 100.0 / nullIf(countIf(action = 'view'), 0), 2) AS like_rate_pct
FROM analytics.user_events
WHERE event_date = today()
GROUP BY content_id
ORDER BY views DESC
LIMIT 10;


-- ------------------------------------------------------------
-- 3. Unique active users per hour (last 12 hours)
-- ------------------------------------------------------------
SELECT
    toStartOfHour(timestamp)    AS hour,
    uniq(user_id)               AS unique_users,
    count()                     AS total_events,
    round(count() / uniq(user_id), 2) AS events_per_user
FROM analytics.user_events
WHERE timestamp >= now() - INTERVAL 12 HOUR
GROUP BY hour
ORDER BY hour DESC;


-- ------------------------------------------------------------
-- 4. Engagement rate by region
-- ------------------------------------------------------------
SELECT
    region,
    count()                         AS total_events,
    uniq(user_id)                   AS unique_users,
    countIf(action = 'view')        AS views,
    countIf(action = 'like')        AS likes,
    countIf(action = 'share')       AS shares,
    round(countIf(action IN ('like', 'share', 'comment')) * 100.0
          / nullIf(countIf(action = 'view'), 0), 2) AS engagement_rate_pct
FROM analytics.user_events
WHERE event_date = today()
GROUP BY region
ORDER BY engagement_rate_pct DESC;


-- ------------------------------------------------------------
-- 5. Real-time event throughput (events per minute, last hour)
-- ------------------------------------------------------------
SELECT
    toStartOfMinute(timestamp)  AS minute,
    count()                     AS events_per_minute,
    uniq(user_id)               AS active_users
FROM analytics.user_events
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute DESC
LIMIT 60;


-- ------------------------------------------------------------
-- 6. Platform breakdown (mobile vs web vs tablet)
-- ------------------------------------------------------------
SELECT
    platform,
    count()         AS total_events,
    uniq(user_id)   AS unique_users,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS share_pct
FROM analytics.user_events
WHERE event_date = today()
GROUP BY platform
ORDER BY total_events DESC;


-- ------------------------------------------------------------
-- 7. Average session engagement depth
-- ------------------------------------------------------------
SELECT
    user_id,
    count()                         AS total_actions,
    uniq(content_id)                AS unique_content_viewed,
    countIf(action = 'view')        AS views,
    countIf(action = 'like')        AS likes,
    round(avg(duration_seconds), 0) AS avg_view_duration_secs
FROM analytics.user_events
WHERE event_date = today()
  AND action = 'view'
  AND duration_seconds IS NOT NULL
GROUP BY user_id
HAVING total_actions > 3
ORDER BY total_actions DESC
LIMIT 20;
