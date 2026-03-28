create procedure upload_csv_to_table(in csv_file_path text)
    LANGUAGE plpgsql
as
$$
BEGIN
    EXECUTE (format($fmt$
        COPY mock_data (
            external_id,
            customer_first_name,
            customer_last_name,
            customer_age,
            customer_email,
            customer_country,
            customer_postal_code,
            customer_pet_type,
            customer_pet_name,
            customer_pet_breed,
            seller_first_name,
            seller_last_name,
            seller_email,
            seller_country,
            seller_postal_code,
            product_name,
            product_category,
            product_price,
            product_quantity,
            sale_date,
            sale_customer_id,
            sale_seller_id,
            sale_product_id,
            sale_quantity,
            sale_total_price,
            store_name,
            store_location,
            store_city,
            store_state,
            store_country,
            store_phone,
            store_email,
            pet_category,
            product_weight,
            product_color,
            product_size,
            product_brand,
            product_material,
            product_description,
            product_rating,
            product_reviews,
            product_release_date,
            product_expiry_date,
            supplier_name,
            supplier_contact,
            supplier_email,
            supplier_phone,
            supplier_address,
            supplier_city,
            supplier_country
        )
        FROM %L
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            NULL ''
        )
    $fmt$, csv_file_path));
END;
$$;

create procedure process_pets()
    LANGUAGE plpgsql
as
$$
BEGIN
    INSERT INTO customer_pets_dim
        (id, name, breed, type)
    SELECT DISTINCT customer_pet_inner_id,
                    customer_pet_name,
                    customer_pet_breed,
                    customer_pet_type
    FROM mock_data;
END;
$$;

create procedure process_customers()
    LANGUAGE plpgsql
as
$$
BEGIN
    INSERT INTO customers_dim
        (first_name, last_name, email, postal_code, country, age, pet_id)
    SELECT mock_data.customer_first_name,
           mock_data.customer_last_name,
           mock_data.customer_email,
           mock_data.customer_postal_code,
           mock_data.customer_country,
           mock_data.customer_age,
           customer_pet_inner_id
    FROM mock_data;
END;
$$;

create procedure process_sellers()
    LANGUAGE plpgsql
as
$$
BEGIN
    INSERT INTO sellers_dim
        (first_name, last_name, email, postal_code, country)
    SELECT mock_data.seller_first_name,
           mock_data.seller_last_name,
           mock_data.seller_email,
           mock_data.seller_postal_code,
           mock_data.seller_country
    FROM mock_data;
END;
$$;

create procedure process_suppliers()
    LANGUAGE plpgsql
as
$$
BEGIN
    INSERT INTO suppliers_dim
        (id, name, contact, email, phone, address, city, country)
    SELECT mock_data.supplier_inner_id,
           mock_data.supplier_name,
           mock_data.supplier_contact,
           mock_data.supplier_email,
           mock_data.supplier_phone,
           mock_data.supplier_address,
           mock_data.supplier_city,
           mock_data.supplier_country
    FROM mock_data;
END;
$$;

create function reindex(in batch_size integer)
    RETURNS void
    LANGUAGE plpgsql
as
$$
BEGIN
    UPDATE mock_data
    SET sale_customer_id = sale_customer_id + ((id - 1) / batch_size) * batch_size,
        sale_seller_id   = sale_seller_id + ((id - 1) / batch_size) * batch_size,
        sale_product_id  = sale_product_id + ((id - 1) / batch_size) * batch_size;
end;
$$;

create procedure process_stores()
    LANGUAGE plpgsql
as
$$
BEGIN
    INSERT INTO stores_dim
        (id, city, country, email, location, name, phone, state)
    SELECT mock_data.store_inner_id,
           mock_data.store_city,
           mock_data.store_country,
           mock_data.store_email,
           mock_data.store_location,
           mock_data.store_name,
           mock_data.store_phone,
           mock_data.store_state
    FROM mock_data;
END;
$$;

create procedure process_products()
    LANGUAGE plpgsql
as
$$
BEGIN
    INSERT INTO products_dim
    (id, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date, category, name,
     price, quantity)
    SELECT mock_data.product_inner_id,
           mock_data.product_weight,
           mock_data.product_color,
           mock_data.product_size,
           mock_data.product_brand,
           mock_data.product_material,
           mock_data.product_description,
           mock_data.product_rating,
           mock_data.product_reviews,
           mock_data.product_release_date,
           mock_data.product_expiry_date,
           mock_data.product_category,
           mock_data.product_name,
           mock_data.product_price,
           mock_data.product_quantity
    FROM mock_data;
END;
$$;

create procedure process_sales()
    LANGUAGE plpgsql
as
$$
BEGIN
    INSERT INTO transactions_fact
    (customer_id, date, product_id, quantity, seller_id, store_id, total_price, pet_category, supplier_id)
    SELECT mock_data.sale_customer_id,
           mock_data.sale_date,
           mock_data.sale_product_id,
           mock_data.sale_quantity,
           mock_data.sale_seller_id,
           mock_data.store_inner_id,
           mock_data.sale_total_price,
           mock_data.pet_category,
           mock_data.supplier_inner_id
    FROM mock_data;
END;
$$


