-- Use default database
DROP TABLE IF EXISTS mock_data_ch;

CREATE TABLE mock_data_ch (
    id UInt32,
    external_id Nullable(Int32),
    customer_inner_id UInt32,
    customer_first_name Nullable(String),
    customer_last_name Nullable(String),
    customer_age Nullable(Int32),
    customer_email Nullable(String),
    customer_country Nullable(String),
    customer_postal_code Nullable(String),
    customer_pet_inner_id UInt32,
    customer_pet_type Nullable(String),
    customer_pet_name Nullable(String),
    customer_pet_breed Nullable(String),
    seller_inner_id UInt32,
    seller_first_name Nullable(String),
    seller_last_name Nullable(String),
    seller_email Nullable(String),
    seller_country Nullable(String),
    seller_postal_code Nullable(String),
    product_inner_id UInt32,
    product_name Nullable(String),
    product_category Nullable(String),
    product_price Nullable(Float64),
    product_quantity Nullable(Int32),
    sale_date Nullable(Date),
    sale_customer_id Nullable(Int32),
    sale_seller_id Nullable(Int32),
    sale_product_id Nullable(Int32),
    sale_quantity Nullable(Int32),
    sale_total_price Nullable(Float64),
    store_inner_id UInt32,
    store_name Nullable(String),
    store_location Nullable(String),
    store_city Nullable(String),
    store_state Nullable(String),
    store_country Nullable(String),
    store_phone Nullable(String),
    store_email Nullable(String),
    pet_category Nullable(String),
    product_weight Nullable(Float64),
    product_color Nullable(String),
    product_size Nullable(String),
    product_brand Nullable(String),
    product_material Nullable(String),
    product_description Nullable(String),
    product_rating Nullable(Float64),
    product_reviews Nullable(Int32),
    product_release_date Nullable(Date),
    product_expiry_date Nullable(Date),
    supplier_inner_id UInt32,
    supplier_name Nullable(String),
    supplier_contact Nullable(String),
    supplier_email Nullable(String),
    supplier_phone Nullable(String),
    supplier_address Nullable(String),
    supplier_city Nullable(String),
    supplier_country Nullable(String)
) ENGINE = MergeTree
ORDER BY id;


INSERT INTO mock_data_ch
SELECT
    generated_id AS id,
    toInt32OrNull(nullIf(external_id, '')) AS external_id,
    generated_id AS customer_inner_id,
    nullIf(customer_first_name, '') AS customer_first_name,
    nullIf(customer_last_name, '') AS customer_last_name,
    toInt32OrNull(nullIf(customer_age, '')) AS customer_age,
    nullIf(customer_email, '') AS customer_email,
    nullIf(customer_country, '') AS customer_country,
    nullIf(customer_postal_code, '') AS customer_postal_code,
    generated_id AS customer_pet_inner_id,
    nullIf(customer_pet_type, '') AS customer_pet_type,
    nullIf(customer_pet_name, '') AS customer_pet_name,
    nullIf(customer_pet_breed, '') AS customer_pet_breed,
    generated_id AS seller_inner_id,
    nullIf(seller_first_name, '') AS seller_first_name,
    nullIf(seller_last_name, '') AS seller_last_name,
    nullIf(seller_email, '') AS seller_email,
    nullIf(seller_country, '') AS seller_country,
    nullIf(seller_postal_code, '') AS seller_postal_code,
    generated_id AS product_inner_id,
    nullIf(product_name, '') AS product_name,
    nullIf(product_category, '') AS product_category,
    toFloat64OrNull(nullIf(product_price, '')) AS product_price,
    toInt32OrNull(nullIf(product_quantity, '')) AS product_quantity,
    CAST(parseDateTimeBestEffortOrNull(nullIf(sale_date, '')) AS Nullable(Date)) AS sale_date,
    toInt32OrNull(nullIf(sale_customer_id, '')) AS sale_customer_id,
    toInt32OrNull(nullIf(sale_seller_id, '')) AS sale_seller_id,
    toInt32OrNull(nullIf(sale_product_id, '')) AS sale_product_id,
    toInt32OrNull(nullIf(sale_quantity, '')) AS sale_quantity,
    toFloat64OrNull(nullIf(sale_total_price, '')) AS sale_total_price,
    generated_id AS store_inner_id,
    nullIf(store_name, '') AS store_name,
    nullIf(store_location, '') AS store_location,
    nullIf(store_city, '') AS store_city,
    nullIf(store_state, '') AS store_state,
    nullIf(store_country, '') AS store_country,
    nullIf(store_phone, '') AS store_phone,
    nullIf(store_email, '') AS store_email,
    nullIf(pet_category, '') AS pet_category,
    toFloat64OrNull(nullIf(product_weight, '')) AS product_weight,
    nullIf(product_color, '') AS product_color,
    nullIf(product_size, '') AS product_size,
    nullIf(product_brand, '') AS product_brand,
    nullIf(product_material, '') AS product_material,
    nullIf(product_description, '') AS product_description,
    toFloat64OrNull(nullIf(product_rating, '')) AS product_rating,
    toInt32OrNull(nullIf(product_reviews, '')) AS product_reviews,
    CAST(parseDateTimeBestEffortOrNull(nullIf(product_release_date, '')) AS Nullable(Date)) AS product_release_date,
    CAST(parseDateTimeBestEffortOrNull(nullIf(product_expiry_date, '')) AS Nullable(Date)) AS product_expiry_date,
    generated_id AS supplier_inner_id,
    nullIf(supplier_name, '') AS supplier_name,
    nullIf(supplier_contact, '') AS supplier_contact,
    nullIf(supplier_email, '') AS supplier_email,
    nullIf(supplier_phone, '') AS supplier_phone,
    nullIf(supplier_address, '') AS supplier_address,
    nullIf(supplier_city, '') AS supplier_city,
    nullIf(supplier_country, '') AS supplier_country
FROM (
    SELECT
        row_number() OVER () AS generated_id,
        *
    FROM (
        SELECT * FROM file(
            '/var/lib/clickhouse/user_files/MOCK_DATA.csv',
            'CSVWithNames',
            'external_id String, customer_first_name String, customer_last_name String, customer_age String, customer_email String, customer_country String, customer_postal_code String, customer_pet_type String, customer_pet_name String, customer_pet_breed String, seller_first_name String, seller_last_name String, seller_email String, seller_country String, seller_postal_code String, product_name String, product_category String, product_price String, product_quantity String, sale_date String, sale_customer_id String, sale_seller_id String, sale_product_id String, sale_quantity String, sale_total_price String, store_name String, store_location String, store_city String, store_state String, store_country String, store_phone String, store_email String, pet_category String, product_weight String, product_color String, product_size String, product_brand String, product_material String, product_description String, product_rating String, product_reviews String, product_release_date String, product_expiry_date String, supplier_name String, supplier_contact String, supplier_email String, supplier_phone String, supplier_address String, supplier_city String, supplier_country String'
        )
        UNION ALL
        SELECT * FROM file(
            '/var/lib/clickhouse/user_files/MOCK_DATA (1).csv',
            'CSVWithNames',
            'external_id String, customer_first_name String, customer_last_name String, customer_age String, customer_email String, customer_country String, customer_postal_code String, customer_pet_type String, customer_pet_name String, customer_pet_breed String, seller_first_name String, seller_last_name String, seller_email String, seller_country String, seller_postal_code String, product_name String, product_category String, product_price String, product_quantity String, sale_date String, sale_customer_id String, sale_seller_id String, sale_product_id String, sale_quantity String, sale_total_price String, store_name String, store_location String, store_city String, store_state String, store_country String, store_phone String, store_email String, pet_category String, product_weight String, product_color String, product_size String, product_brand String, product_material String, product_description String, product_rating String, product_reviews String, product_release_date String, product_expiry_date String, supplier_name String, supplier_contact String, supplier_email String, supplier_phone String, supplier_address String, supplier_city String, supplier_country String'
        )
        UNION ALL
        SELECT * FROM file(
            '/var/lib/clickhouse/user_files/MOCK_DATA (2).csv',
            'CSVWithNames',
            'external_id String, customer_first_name String, customer_last_name String, customer_age String, customer_email String, customer_country String, customer_postal_code String, customer_pet_type String, customer_pet_name String, customer_pet_breed String, seller_first_name String, seller_last_name String, seller_email String, seller_country String, seller_postal_code String, product_name String, product_category String, product_price String, product_quantity String, sale_date String, sale_customer_id String, sale_seller_id String, sale_product_id String, sale_quantity String, sale_total_price String, store_name String, store_location String, store_city String, store_state String, store_country String, store_phone String, store_email String, pet_category String, product_weight String, product_color String, product_size String, product_brand String, product_material String, product_description String, product_rating String, product_reviews String, product_release_date String, product_expiry_date String, supplier_name String, supplier_contact String, supplier_email String, supplier_phone String, supplier_address String, supplier_city String, supplier_country String'
        )
        UNION ALL
        SELECT * FROM file(
            '/var/lib/clickhouse/user_files/MOCK_DATA (3).csv',
            'CSVWithNames',
            'external_id String, customer_first_name String, customer_last_name String, customer_age String, customer_email String, customer_country String, customer_postal_code String, customer_pet_type String, customer_pet_name String, customer_pet_breed String, seller_first_name String, seller_last_name String, seller_email String, seller_country String, seller_postal_code String, product_name String, product_category String, product_price String, product_quantity String, sale_date String, sale_customer_id String, sale_seller_id String, sale_product_id String, sale_quantity String, sale_total_price String, store_name String, store_location String, store_city String, store_state String, store_country String, store_phone String, store_email String, pet_category String, product_weight String, product_color String, product_size String, product_brand String, product_material String, product_description String, product_rating String, product_reviews String, product_release_date String, product_expiry_date String, supplier_name String, supplier_contact String, supplier_email String, supplier_phone String, supplier_address String, supplier_city String, supplier_country String'
        )
        UNION ALL
        SELECT * FROM file(
            '/var/lib/clickhouse/user_files/MOCK_DATA (4).csv',
            'CSVWithNames',
            'external_id String, customer_first_name String, customer_last_name String, customer_age String, customer_email String, customer_country String, customer_postal_code String, customer_pet_type String, customer_pet_name String, customer_pet_breed String, seller_first_name String, seller_last_name String, seller_email String, seller_country String, seller_postal_code String, product_name String, product_category String, product_price String, product_quantity String, sale_date String, sale_customer_id String, sale_seller_id String, sale_product_id String, sale_quantity String, sale_total_price String, store_name String, store_location String, store_city String, store_state String, store_country String, store_phone String, store_email String, pet_category String, product_weight String, product_color String, product_size String, product_brand String, product_material String, product_description String, product_rating String, product_reviews String, product_release_date String, product_expiry_date String, supplier_name String, supplier_contact String, supplier_email String, supplier_phone String, supplier_address String, supplier_city String, supplier_country String'
        )
    )
);
