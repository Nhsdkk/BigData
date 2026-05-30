CREATE SCHEMA IF NOT EXISTS clickhouse.dwh;

DROP TABLE IF EXISTS clickhouse.dwh.transactions_fact;
DROP TABLE IF EXISTS clickhouse.dwh.customers_dim;
DROP TABLE IF EXISTS clickhouse.dwh.customer_pets_dim;
DROP TABLE IF EXISTS clickhouse.dwh.sellers_dim;
DROP TABLE IF EXISTS clickhouse.dwh.suppliers_dim;
DROP TABLE IF EXISTS clickhouse.dwh.products_dim;
DROP TABLE IF EXISTS clickhouse.dwh.stores_dim;
DROP TABLE IF EXISTS clickhouse.dwh.sales_staging;

CREATE TABLE clickhouse.dwh.customer_pets_dim (
                                                  id BIGINT NOT NULL,
                                                  "name" VARCHAR,
                                                  breed VARCHAR,
                                                  "type" VARCHAR
)
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['id']
        );

CREATE TABLE clickhouse.dwh.customers_dim (
                                              id BIGINT NOT NULL,
                                              first_name VARCHAR,
                                              last_name VARCHAR,
                                              email VARCHAR,
                                              postal_code VARCHAR,
                                              country VARCHAR,
                                              age INTEGER,
                                              pet_id BIGINT
)
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['id']
        );

CREATE TABLE clickhouse.dwh.sellers_dim (
                                            id BIGINT NOT NULL,
                                            first_name VARCHAR,
                                            last_name VARCHAR,
                                            email VARCHAR,
                                            postal_code VARCHAR,
                                            country VARCHAR
)
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['id']
        );

CREATE TABLE clickhouse.dwh.suppliers_dim (
                                              id BIGINT NOT NULL,
                                              address VARCHAR,
                                              city VARCHAR,
                                              contact VARCHAR,
                                              country VARCHAR,
                                              email VARCHAR,
                                              "name" VARCHAR,
                                              phone VARCHAR
)
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['id']
        );

CREATE TABLE clickhouse.dwh.products_dim (
                                             id BIGINT NOT NULL,
                                             weight DOUBLE,
                                             color VARCHAR,
                                             size VARCHAR,
                                             brand VARCHAR,
                                             material VARCHAR,
                                             description VARCHAR,
                                             rating DOUBLE,
                                             reviews INTEGER,
                                             release_date DATE,
                                             expiry_date DATE,
                                             category VARCHAR,
                                             "name" VARCHAR,
                                             price DOUBLE,
                                             quantity INTEGER
)
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['id']
        );

CREATE TABLE clickhouse.dwh.stores_dim (
                                           id BIGINT NOT NULL,
                                           city VARCHAR,
                                           country VARCHAR,
                                           email VARCHAR,
                                           location VARCHAR,
                                           "name" VARCHAR,
                                           phone VARCHAR,
                                           state VARCHAR
)
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['id']
        );

CREATE TABLE clickhouse.dwh.transactions_fact (
                                                  id BIGINT NOT NULL,
                                                  customer_id BIGINT,
                                                  "date" DATE,
                                                  product_id BIGINT,
                                                  quantity INTEGER,
                                                  seller_id BIGINT,
                                                  store_id BIGINT,
                                                  total_price DOUBLE,
                                                  pet_category VARCHAR,
                                                  supplier_id BIGINT,
                                                  source_system VARCHAR,
                                                  source_row_id BIGINT
)
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['id']
        );


INSERT INTO clickhouse.dwh.customer_pets_dim (
    id,
    "name",
    breed,
    "type"
)
WITH raw_union AS (
    SELECT
        'clickhouse' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(customer_pet_inner_id AS BIGINT) AS customer_pet_inner_id,
        CAST(customer_pet_type AS VARCHAR) AS customer_pet_type,
        CAST(customer_pet_name AS VARCHAR) AS customer_pet_name,
        CAST(customer_pet_breed AS VARCHAR) AS customer_pet_breed
    FROM clickhouse.default.mock_data_ch

    UNION ALL

    SELECT
        'postgresql' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(customer_pet_inner_id AS BIGINT) AS customer_pet_inner_id,
        CAST(customer_pet_type AS VARCHAR) AS customer_pet_type,
        CAST(customer_pet_name AS VARCHAR) AS customer_pet_name,
        CAST(customer_pet_breed AS VARCHAR) AS customer_pet_breed
    FROM postgresql.raw_pg.mock_data
),
     normalized AS (
         SELECT
             *,
             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(customer_pet_inner_id AS VARCHAR)))
                             )
                     )
             ) AS customer_pet_id
         FROM raw_union
     ),
     dedup AS (
         SELECT
             *,
             row_number() OVER (
                 PARTITION BY customer_pet_id
                 ORDER BY source_row_id
                 ) AS rn
         FROM normalized
     )
SELECT
    customer_pet_id AS id,
    customer_pet_name AS "name",
    customer_pet_breed AS breed,
    customer_pet_type AS "type"
FROM dedup
WHERE rn = 1;


INSERT INTO clickhouse.dwh.customers_dim (
    id,
    first_name,
    last_name,
    email,
    postal_code,
    country,
    age,
    pet_id
)
WITH raw_union AS (
    SELECT
        'clickhouse' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(customer_inner_id AS BIGINT) AS customer_inner_id,
        CAST(customer_first_name AS VARCHAR) AS customer_first_name,
        CAST(customer_last_name AS VARCHAR) AS customer_last_name,
        CAST(customer_age AS INTEGER) AS customer_age,
        CAST(customer_email AS VARCHAR) AS customer_email,
        CAST(customer_country AS VARCHAR) AS customer_country,
        CAST(customer_postal_code AS VARCHAR) AS customer_postal_code,
        CAST(customer_pet_inner_id AS BIGINT) AS customer_pet_inner_id
    FROM clickhouse.default.mock_data_ch

    UNION ALL

    SELECT
        'postgresql' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(customer_inner_id AS BIGINT) AS customer_inner_id,
        CAST(customer_first_name AS VARCHAR) AS customer_first_name,
        CAST(customer_last_name AS VARCHAR) AS customer_last_name,
        CAST(customer_age AS INTEGER) AS customer_age,
        CAST(customer_email AS VARCHAR) AS customer_email,
        CAST(customer_country AS VARCHAR) AS customer_country,
        CAST(customer_postal_code AS VARCHAR) AS customer_postal_code,
        CAST(customer_pet_inner_id AS BIGINT) AS customer_pet_inner_id
    FROM postgresql.raw_pg.mock_data
),
     normalized AS (
         SELECT
             *,
             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(customer_inner_id AS VARCHAR)))
                             )
                     )
             ) AS customer_id,
             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(customer_pet_inner_id AS VARCHAR)))
                             )
                     )
             ) AS customer_pet_id
         FROM raw_union
     ),
     dedup AS (
         SELECT
             *,
             row_number() OVER (
                 PARTITION BY customer_id
                 ORDER BY source_row_id
                 ) AS rn
         FROM normalized
     )
SELECT
    customer_id AS id,
    customer_first_name AS first_name,
    customer_last_name AS last_name,
    lower(customer_email) AS email,
    customer_postal_code AS postal_code,
    customer_country AS country,
    customer_age AS age,
    customer_pet_id AS pet_id
FROM dedup
WHERE rn = 1;


INSERT INTO clickhouse.dwh.sellers_dim (
    id,
    first_name,
    last_name,
    email,
    postal_code,
    country
)
WITH raw_union AS (
    SELECT
        'clickhouse' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(seller_inner_id AS BIGINT) AS seller_inner_id,
        CAST(seller_first_name AS VARCHAR) AS seller_first_name,
        CAST(seller_last_name AS VARCHAR) AS seller_last_name,
        CAST(seller_email AS VARCHAR) AS seller_email,
        CAST(seller_country AS VARCHAR) AS seller_country,
        CAST(seller_postal_code AS VARCHAR) AS seller_postal_code
    FROM clickhouse.default.mock_data_ch

    UNION ALL

    SELECT
        'postgresql' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(seller_inner_id AS BIGINT) AS seller_inner_id,
        CAST(seller_first_name AS VARCHAR) AS seller_first_name,
        CAST(seller_last_name AS VARCHAR) AS seller_last_name,
        CAST(seller_email AS VARCHAR) AS seller_email,
        CAST(seller_country AS VARCHAR) AS seller_country,
        CAST(seller_postal_code AS VARCHAR) AS seller_postal_code
    FROM postgresql.raw_pg.mock_data
),
     normalized AS (
         SELECT
             *,
             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(seller_inner_id AS VARCHAR)))
                             )
                     )
             ) AS seller_id
         FROM raw_union
     ),
     dedup AS (
         SELECT
             *,
             row_number() OVER (
                 PARTITION BY seller_id
                 ORDER BY source_row_id
                 ) AS rn
         FROM normalized
     )
SELECT
    seller_id AS id,
    seller_first_name AS first_name,
    seller_last_name AS last_name,
    lower(seller_email) AS email,
    seller_postal_code AS postal_code,
    seller_country AS country
FROM dedup
WHERE rn = 1;


INSERT INTO clickhouse.dwh.suppliers_dim (
    id,
    address,
    city,
    contact,
    country,
    email,
    "name",
    phone
)
WITH raw_union AS (
    SELECT
        'clickhouse' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(supplier_inner_id AS BIGINT) AS supplier_inner_id,
        CAST(supplier_name AS VARCHAR) AS supplier_name,
        CAST(supplier_contact AS VARCHAR) AS supplier_contact,
        CAST(supplier_email AS VARCHAR) AS supplier_email,
        CAST(supplier_phone AS VARCHAR) AS supplier_phone,
        CAST(supplier_address AS VARCHAR) AS supplier_address,
        CAST(supplier_city AS VARCHAR) AS supplier_city,
        CAST(supplier_country AS VARCHAR) AS supplier_country
    FROM clickhouse.default.mock_data_ch

    UNION ALL

    SELECT
        'postgresql' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(supplier_inner_id AS BIGINT) AS supplier_inner_id,
        CAST(supplier_name AS VARCHAR) AS supplier_name,
        CAST(supplier_contact AS VARCHAR) AS supplier_contact,
        CAST(supplier_email AS VARCHAR) AS supplier_email,
        CAST(supplier_phone AS VARCHAR) AS supplier_phone,
        CAST(supplier_address AS VARCHAR) AS supplier_address,
        CAST(supplier_city AS VARCHAR) AS supplier_city,
        CAST(supplier_country AS VARCHAR) AS supplier_country
    FROM postgresql.raw_pg.mock_data
),
     normalized AS (
         SELECT
             *,
             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(supplier_inner_id AS VARCHAR)))
                             )
                     )
             ) AS supplier_id
         FROM raw_union
     ),
     dedup AS (
         SELECT
             *,
             row_number() OVER (
                 PARTITION BY supplier_id
                 ORDER BY source_row_id
                 ) AS rn
         FROM normalized
     )
SELECT
    supplier_id AS id,
    supplier_address AS address,
    supplier_city AS city,
    supplier_contact AS contact,
    supplier_country AS country,
    lower(supplier_email) AS email,
    supplier_name AS "name",
    supplier_phone AS phone
FROM dedup
WHERE rn = 1;


INSERT INTO clickhouse.dwh.products_dim (
    id,
    weight,
    color,
    size,
    brand,
    material,
    description,
    rating,
    reviews,
    release_date,
    expiry_date,
    category,
    "name",
    price,
    quantity
)
WITH raw_union AS (
    SELECT
        'clickhouse' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(product_inner_id AS BIGINT) AS product_inner_id,
        CAST(product_name AS VARCHAR) AS product_name,
        CAST(product_category AS VARCHAR) AS product_category,
        CAST(product_price AS DOUBLE) AS product_price,
        CAST(product_quantity AS INTEGER) AS product_quantity,
        CAST(product_weight AS DOUBLE) AS product_weight,
        CAST(product_color AS VARCHAR) AS product_color,
        CAST(product_size AS VARCHAR) AS product_size,
        CAST(product_brand AS VARCHAR) AS product_brand,
        CAST(product_material AS VARCHAR) AS product_material,
        CAST(product_description AS VARCHAR) AS product_description,
        CAST(product_rating AS DOUBLE) AS product_rating,
        CAST(product_reviews AS INTEGER) AS product_reviews,
        CAST(product_release_date AS DATE) AS product_release_date,
        CAST(product_expiry_date AS DATE) AS product_expiry_date
    FROM clickhouse.default.mock_data_ch

    UNION ALL

    SELECT
        'postgresql' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(product_inner_id AS BIGINT) AS product_inner_id,
        CAST(product_name AS VARCHAR) AS product_name,
        CAST(product_category AS VARCHAR) AS product_category,
        CAST(product_price AS DOUBLE) AS product_price,
        CAST(product_quantity AS INTEGER) AS product_quantity,
        CAST(product_weight AS DOUBLE) AS product_weight,
        CAST(product_color AS VARCHAR) AS product_color,
        CAST(product_size AS VARCHAR) AS product_size,
        CAST(product_brand AS VARCHAR) AS product_brand,
        CAST(product_material AS VARCHAR) AS product_material,
        CAST(product_description AS VARCHAR) AS product_description,
        CAST(product_rating AS DOUBLE) AS product_rating,
        CAST(product_reviews AS INTEGER) AS product_reviews,
        CAST(product_release_date AS DATE) AS product_release_date,
        CAST(product_expiry_date AS DATE) AS product_expiry_date
    FROM postgresql.raw_pg.mock_data
),
     normalized AS (
         SELECT
             *,
             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(product_inner_id AS VARCHAR)))
                             )
                     )
             ) AS product_id
         FROM raw_union
     ),
     dedup AS (
         SELECT
             *,
             row_number() OVER (
                 PARTITION BY product_id
                 ORDER BY source_row_id
                 ) AS rn
         FROM normalized
     )
SELECT
    product_id AS id,
    product_weight AS weight,
    product_color AS color,
    product_size AS size,
    product_brand AS brand,
    product_material AS material,
    product_description AS description,
    product_rating AS rating,
    product_reviews AS reviews,
    product_release_date AS release_date,
    product_expiry_date AS expiry_date,
    product_category AS category,
    product_name AS "name",
    product_price AS price,
    product_quantity AS quantity
FROM dedup
WHERE rn = 1;


INSERT INTO clickhouse.dwh.stores_dim (
    id,
    city,
    country,
    email,
    location,
    "name",
    phone,
    state
)
WITH raw_union AS (
    SELECT
        'clickhouse' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(store_inner_id AS BIGINT) AS store_inner_id,
        CAST(store_name AS VARCHAR) AS store_name,
        CAST(store_location AS VARCHAR) AS store_location,
        CAST(store_city AS VARCHAR) AS store_city,
        CAST(store_state AS VARCHAR) AS store_state,
        CAST(store_country AS VARCHAR) AS store_country,
        CAST(store_phone AS VARCHAR) AS store_phone,
        CAST(store_email AS VARCHAR) AS store_email
    FROM clickhouse.default.mock_data_ch

    UNION ALL

    SELECT
        'postgresql' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(store_inner_id AS BIGINT) AS store_inner_id,
        CAST(store_name AS VARCHAR) AS store_name,
        CAST(store_location AS VARCHAR) AS store_location,
        CAST(store_city AS VARCHAR) AS store_city,
        CAST(store_state AS VARCHAR) AS store_state,
        CAST(store_country AS VARCHAR) AS store_country,
        CAST(store_phone AS VARCHAR) AS store_phone,
        CAST(store_email AS VARCHAR) AS store_email
    FROM postgresql.raw_pg.mock_data
),
     normalized AS (
         SELECT
             *,
             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(store_inner_id AS VARCHAR)))
                             )
                     )
             ) AS store_id
         FROM raw_union
     ),
     dedup AS (
         SELECT
             *,
             row_number() OVER (
                 PARTITION BY store_id
                 ORDER BY source_row_id
                 ) AS rn
         FROM normalized
     )
SELECT
    store_id AS id,
    store_city AS city,
    store_country AS country,
    lower(store_email) AS email,
    store_location AS location,
    store_name AS "name",
    store_phone AS phone,
    store_state AS state
FROM dedup
WHERE rn = 1;


INSERT INTO clickhouse.dwh.transactions_fact (
    id,
    customer_id,
    "date",
    product_id,
    quantity,
    seller_id,
    store_id,
    total_price,
    pet_category,
    supplier_id,
    source_system,
    source_row_id
)
WITH raw_union AS (
    SELECT
        'clickhouse' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(sale_customer_id AS BIGINT) AS sale_customer_id,
        CAST(sale_seller_id AS BIGINT) AS sale_seller_id,
        CAST(sale_product_id AS BIGINT) AS sale_product_id,
        CAST(sale_date AS DATE) AS sale_date,
        CAST(sale_quantity AS INTEGER) AS sale_quantity,
        CAST(sale_total_price AS DOUBLE) AS sale_total_price,
        CAST(store_inner_id AS BIGINT) AS store_inner_id,
        CAST(pet_category AS VARCHAR) AS pet_category,
        CAST(supplier_inner_id AS BIGINT) AS supplier_inner_id
    FROM clickhouse.default.mock_data_ch

    UNION ALL

    SELECT
        'postgresql' AS source_system,
        CAST(id AS BIGINT) AS source_row_id,
        CAST(sale_customer_id AS BIGINT) AS sale_customer_id,
        CAST(sale_seller_id AS BIGINT) AS sale_seller_id,
        CAST(sale_product_id AS BIGINT) AS sale_product_id,
        CAST(sale_date AS DATE) AS sale_date,
        CAST(sale_quantity AS INTEGER) AS sale_quantity,
        CAST(sale_total_price AS DOUBLE) AS sale_total_price,
        CAST(store_inner_id AS BIGINT) AS store_inner_id,
        CAST(pet_category AS VARCHAR) AS pet_category,
        CAST(supplier_inner_id AS BIGINT) AS supplier_inner_id
    FROM postgresql.raw_pg.mock_data
),
     normalized AS (
         SELECT
             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(source_row_id AS VARCHAR)))
                             )
                     )
             ) AS transaction_id,

             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(sale_customer_id AS VARCHAR)))
                             )
                     )
             ) AS customer_id,

             sale_date,

             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(sale_product_id AS VARCHAR)))
                             )
                     )
             ) AS product_id,

             sale_quantity,

             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(sale_seller_id AS VARCHAR)))
                             )
                     )
             ) AS seller_id,

             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(store_inner_id AS VARCHAR)))
                             )
                     )
             ) AS store_id,

             sale_total_price,
             pet_category,

             abs(
                     from_big_endian_64(
                             xxhash64(
                                     to_utf8(concat(source_system, '|', CAST(supplier_inner_id AS VARCHAR)))
                             )
                     )
             ) AS supplier_id,

             source_system,
             source_row_id
         FROM raw_union
     )
SELECT
    transaction_id AS id,
    customer_id,
    sale_date AS "date",
    product_id,
    sale_quantity AS quantity,
    seller_id,
    store_id,
    sale_total_price AS total_price,
    pet_category,
    supplier_id,
    source_system,
    source_row_id
FROM normalized;


SELECT 'customer_pets_dim' AS table_name, count(*) AS rows_loaded
FROM clickhouse.dwh.customer_pets_dim

UNION ALL

SELECT 'customers_dim' AS table_name, count(*) AS rows_loaded
FROM clickhouse.dwh.customers_dim

UNION ALL

SELECT 'sellers_dim' AS table_name, count(*) AS rows_loaded
FROM clickhouse.dwh.sellers_dim

UNION ALL

SELECT 'suppliers_dim' AS table_name, count(*) AS rows_loaded
FROM clickhouse.dwh.suppliers_dim

UNION ALL

SELECT 'products_dim' AS table_name, count(*) AS rows_loaded
FROM clickhouse.dwh.products_dim

UNION ALL

SELECT 'stores_dim' AS table_name, count(*) AS rows_loaded
FROM clickhouse.dwh.stores_dim

UNION ALL

SELECT 'transactions_fact' AS table_name, count(*) AS rows_loaded
FROM clickhouse.dwh.transactions_fact;