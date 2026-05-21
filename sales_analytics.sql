-- E-Commerce Sales Analytics Queries for Amazon Redshift

-- 1. Revenue by region
SELECT c.region, COUNT(f.order_id) AS orders,
       ROUND(SUM(f.total_amount), 2) AS revenue
FROM ecommerce_analytics.fact_sales f
JOIN ecommerce_analytics.dim_customers c ON f.customer_id = c.customer_id
WHERE f.status = 'completed'
GROUP BY c.region ORDER BY revenue DESC;

-- 2. Monthly sales trend
SELECT f.order_year, f.order_month,
       COUNT(f.order_id) AS orders,
       ROUND(SUM(f.total_amount), 2) AS revenue
FROM ecommerce_analytics.fact_sales f
WHERE f.status = 'completed'
GROUP BY f.order_year, f.order_month
ORDER BY f.order_year, f.order_month;

-- 3. Top products by category
SELECT p.category, p.name, ROUND(SUM(f.total_amount), 2) AS revenue
FROM ecommerce_analytics.fact_sales f
JOIN ecommerce_analytics.dim_products p ON f.product_id = p.product_id
WHERE f.status = 'completed'
GROUP BY p.category, p.name ORDER BY revenue DESC LIMIT 10;
