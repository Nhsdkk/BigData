CREATE SCHEMA IF NOT EXISTS clickhouse.mart;

DROP TABLE IF EXISTS clickhouse.mart.report_product_sales;
DROP TABLE IF EXISTS clickhouse.mart.report_customer_sales;
DROP TABLE IF EXISTS clickhouse.mart.report_time_sales;
DROP TABLE IF EXISTS clickhouse.mart.report_store_sales;
DROP TABLE IF EXISTS clickhouse.mart.report_supplier_sales;
DROP TABLE IF EXISTS clickhouse.mart.report_product_quality;

CREATE TABLE clickhouse.mart.report_product_sales AS
WITH product_agg AS (
    SELECT
        p."name" AS product_name,
        p.brand AS product_brand,
        p.category AS product_category,
        SUM(f.total_price) AS total_revenue,
        SUM(f.quantity) AS total_quantity_sold,
        SUM(COALESCE(p.reviews, 0)) AS total_review_count,
        SUM(COALESCE(p.rating, 0.0) * COALESCE(p.reviews, 0)) AS total_rating_sum
    FROM clickhouse.dwh.products_dim p
    JOIN clickhouse.dwh.transactions_fact f
        ON p.id = f.product_id
    GROUP BY
        p."name",
        p.brand,
        p.category
)
SELECT
    product_name,
    product_brand,
    product_category,
    total_revenue,
    total_quantity_sold,
    total_review_count,
    CASE
        WHEN total_review_count = 0 THEN 0.0
        ELSE total_rating_sum / total_review_count
    END AS average_rating,
    SUM(total_revenue) OVER (PARTITION BY product_category) AS category_total_revenue,
    ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC, total_revenue DESC, product_name, product_brand) <= 10 AS is_top_10_by_quantity
FROM product_agg;

CREATE TABLE clickhouse.mart.report_customer_sales AS
WITH customer_agg AS (
    SELECT
        c.id AS customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        SUM(f.total_price) AS total_money_spent,
        AVG(f.total_price) AS average_receipt_total
    FROM clickhouse.dwh.customers_dim c
    JOIN clickhouse.dwh.transactions_fact f
        ON c.id = f.customer_id
    GROUP BY
        c.id,
        c.first_name,
        c.last_name,
        c.email,
        c.country
)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    country,
    total_money_spent,
    average_receipt_total,
    COUNT(*) OVER (PARTITION BY country) AS customer_count_by_country,
    ROW_NUMBER() OVER (ORDER BY total_money_spent DESC, customer_id) <= 10 AS is_top_10_customer
FROM customer_agg;

CREATE TABLE clickhouse.mart.report_time_sales AS
WITH monthly_agg AS (
    SELECT
        MONTH("date") AS month,
        YEAR("date") AS year,
        SUM(total_price) AS total_income_per_month,
        COUNT(id) AS total_transactions_per_month,
        SUM(quantity) AS total_sold_items_per_month,
        AVG(quantity) AS average_sold_items_per_month
    FROM clickhouse.dwh.transactions_fact
    GROUP BY
        MONTH("date"),
        YEAR("date")
),
stats AS (
    SELECT
        month,
        year,
        average_sold_items_per_month,
        total_sold_items_per_month,
        total_transactions_per_month,
        total_income_per_month,
        COALESCE(LAG(total_income_per_month) OVER (ORDER BY year, month), 0.0) AS prev_month_income,
        COALESCE(LAG(total_transactions_per_month) OVER (ORDER BY year, month), 0) AS prev_month_transactions_count,
        COALESCE(LAG(total_sold_items_per_month) OVER (ORDER BY year, month), 0) AS prev_month_sold_items_count,
        COALESCE(LAG(total_income_per_month) OVER (PARTITION BY month ORDER BY year), 0.0) AS prev_year_same_month_income,
        COALESCE(LAG(total_transactions_per_month) OVER (PARTITION BY month ORDER BY year), 0) AS prev_year_same_month_transactions_count,
        COALESCE(LAG(total_sold_items_per_month) OVER (PARTITION BY month ORDER BY year), 0) AS prev_year_same_month_sold_items_count
    FROM monthly_agg
)
SELECT
    month,
    year,
    average_sold_items_per_month,
    total_sold_items_per_month,
    total_transactions_per_month,
    total_income_per_month,
    prev_month_income,
    CASE
        WHEN prev_month_income = 0 THEN 0.0
        ELSE ((total_income_per_month - prev_month_income) / prev_month_income) * 100
    END AS percent_diff_prev_month_income,
    prev_year_same_month_income,
    CASE
        WHEN prev_year_same_month_income = 0 THEN 0.0
        ELSE ((total_income_per_month - prev_year_same_month_income) / prev_year_same_month_income) * 100
    END AS percent_diff_prev_year_same_month_income,
    prev_month_transactions_count,
    CASE
        WHEN prev_month_transactions_count = 0 THEN 0.0
        ELSE ((total_transactions_per_month - prev_month_transactions_count) * 100.0) / prev_month_transactions_count
    END AS percent_diff_prev_month_transactions_count,
    prev_year_same_month_transactions_count,
    CASE
        WHEN prev_year_same_month_transactions_count = 0 THEN 0.0
        ELSE ((total_transactions_per_month - prev_year_same_month_transactions_count) * 100.0) / prev_year_same_month_transactions_count
    END AS percent_diff_prev_year_same_month_transactions_count,
    prev_month_sold_items_count,
    CASE
        WHEN prev_month_sold_items_count = 0 THEN 0.0
        ELSE ((total_sold_items_per_month - prev_month_sold_items_count) * 100.0) / prev_month_sold_items_count
    END AS percent_diff_prev_month_sold_items_count,
    prev_year_same_month_sold_items_count,
    CASE
        WHEN prev_year_same_month_sold_items_count = 0 THEN 0.0
        ELSE ((total_sold_items_per_month - prev_year_same_month_sold_items_count) * 100.0) / prev_year_same_month_sold_items_count
    END AS percent_diff_prev_year_same_month_sold_items_count
FROM stats;

CREATE TABLE clickhouse.mart.report_store_sales AS
WITH store_agg AS (
    SELECT
        s.id AS store_id,
        s."name" AS store_name,
        s.email AS store_email,
        s.country,
        s.city,
        SUM(f.total_price) AS total_revenue,
        AVG(f.total_price) AS average_receipt_total,
        COUNT(f.id) AS total_transactions
    FROM clickhouse.dwh.stores_dim s
    JOIN clickhouse.dwh.transactions_fact f
        ON s.id = f.store_id
    GROUP BY
        s.id,
        s."name",
        s.email,
        s.country,
        s.city
)
SELECT
    store_id,
    store_name,
    store_email,
    country,
    city,
    total_revenue,
    average_receipt_total,
    SUM(total_transactions) OVER (PARTITION BY country) AS transactions_count_by_country,
    SUM(total_transactions) OVER (PARTITION BY city) AS transactions_count_by_city,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC, total_transactions DESC, store_id) <= 5 AS is_top_5_store
FROM store_agg;

CREATE TABLE clickhouse.mart.report_supplier_sales AS
WITH supplier_agg AS (
    SELECT
        s.id AS supplier_id,
        s."name" AS supplier_name,
        s.email AS supplier_email,
        s.country,
        SUM(f.total_price) AS total_revenue,
        AVG(p.price) AS average_product_cost,
        COUNT(p.id) AS total_transactions
    FROM clickhouse.dwh.suppliers_dim s
    JOIN clickhouse.dwh.transactions_fact f
        ON s.id = f.supplier_id
    JOIN clickhouse.dwh.products_dim p
        ON p.id = f.product_id
    GROUP BY
        s.id,
        s."name",
        s.email,
        s.country
)
SELECT
    supplier_id,
    supplier_name,
    supplier_email,
    country,
    total_revenue,
    average_product_cost,
    SUM(total_transactions) OVER (PARTITION BY country) AS transactions_count_by_country,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC, total_transactions DESC, supplier_id) <= 5 AS is_top_5_supplier
FROM supplier_agg;

CREATE TABLE clickhouse.mart.report_product_quality AS
WITH product_agg AS (
    SELECT
        p."name" AS product_name,
        p.brand AS product_brand,
        SUM(f.quantity) AS total_sale_count,
        SUM(COALESCE(p.reviews, 0)) AS total_review_count,
        SUM(COALESCE(p.rating, 0.0) * COALESCE(p.reviews, 0)) AS total_rating_sum
    FROM clickhouse.dwh.products_dim p
    JOIN clickhouse.dwh.transactions_fact f
        ON p.id = f.product_id
    GROUP BY
        p."name",
        p.brand
),
quality AS (
    SELECT
        product_name,
        product_brand,
        total_sale_count,
        total_review_count,
        CASE
            WHEN total_review_count = 0 THEN 0.0
            ELSE total_rating_sum / total_review_count
        END AS average_rating
    FROM product_agg
),
stats AS (
    SELECT
        MAX(average_rating) AS max_rating,
        MIN(average_rating) AS min_rating,
        MAX(total_review_count) AS max_review_count,
        CORR(average_rating, CAST(total_sale_count AS double)) AS rating_to_sale_count_corr
    FROM quality
)
SELECT
    q.product_name,
    q.product_brand,
    q.total_review_count,
    q.average_rating,
    q.total_sale_count,
    q.total_review_count = s.max_review_count AS has_max_review_count,
    q.average_rating = s.max_rating AS has_max_average_rating,
    q.average_rating = s.min_rating AS has_min_average_rating,
    s.rating_to_sale_count_corr AS sales_rating_corr_global
FROM quality q
CROSS JOIN stats s;
