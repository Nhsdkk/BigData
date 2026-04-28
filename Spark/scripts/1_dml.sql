create table mock_data
(
    id                    serial NOT NULL PRIMARY KEY,
    external_id           integer,
    customer_inner_id     serial NOT NULL,
    customer_first_name   text,
    customer_last_name    text,
    customer_age          integer,
    customer_email        text,
    customer_country      text,
    customer_postal_code  text,
    customer_pet_inner_id serial NOT NULL,
    customer_pet_type     text,
    customer_pet_name     text,
    customer_pet_breed    text,
    seller_inner_id       serial NOT NULL,
    seller_first_name     text,
    seller_last_name      text,
    seller_email          text,
    seller_country        text,
    seller_postal_code    text,
    product_inner_id      serial NOT NULL,
    product_name          text,
    product_category      text,
    product_price         double precision,
    product_quantity      integer,
    sale_date             date,
    sale_customer_id      integer,
    sale_seller_id        integer,
    sale_product_id       integer,
    sale_quantity         integer,
    sale_total_price      double precision,
    store_inner_id        serial NOT NULL,
    store_name            text,
    store_location        text,
    store_city            text,
    store_state           text,
    store_country         text,
    store_phone           text,
    store_email           text,
    pet_category          text,
    product_weight        double precision,
    product_color         text,
    product_size          text,
    product_brand         text,
    product_material      text,
    product_description   text,
    product_rating        double precision,
    product_reviews       integer,
    product_release_date  date,
    product_expiry_date   date,
    supplier_inner_id     serial NOT NULL,
    supplier_name         text,
    supplier_contact      text,
    supplier_email        text,
    supplier_phone        text,
    supplier_address      text,
    supplier_city         text,
    supplier_country      text
);

create table customer_pets_dim
(
    id    serial NOT NULL PRIMARY KEY,
    name  text   NOT NULL,
    breed text   NOT NULL,
    type  text   NOT NULL
);

create table customers_dim
(
    id          serial  NOT NULL PRIMARY KEY,
    first_name  text    NOT NULL,
    last_name   text    NOT NULL,
    email       text    NOT NULL UNIQUE,
    postal_code text,
    country     text    NOT NULL,
    age         integer NOT NULL,
    pet_id      integer references customer_pets_dim (id) ON DELETE CASCADE
);

create table sellers_dim
(
    id          serial NOT NULL PRIMARY KEY,
    first_name  text   NOT NULL,
    last_name   text   NOT NULL,
    email       text   NOT NULL UNIQUE,
    postal_code text,
    country     text   NOT NULL
);

create table suppliers_dim
(
    id      serial NOT NULL PRIMARY KEY,
    address text   NOT NULL,
    city    text   NOT NULL,
    contact text   NOT NULL,
    country text   NOT NULL,
    email   text   NOT NULL UNIQUE,
    name    text   NOT NULL,
    phone   text   NOT NULL UNIQUE
);

create table products_dim
(
    id           serial           NOT NULL PRIMARY KEY,
    weight       double precision NOT NULL,
    color        text             NOT NULL,
    size         text             NOT NULL,
    brand        text             NOT NULL,
    material     text             NOT NULL,
    description  text             NOT NULL,
    rating       double precision NOT NULL,
    reviews      integer          NOT NULL,
    release_date date             NOT NULL,
    expiry_date  date             NOT NULL,
    category     text             NOT NULL,
    name         text             NOT NULL,
    price        double precision NOT NULL,
    quantity     integer          NOT NULL
);

create table stores_dim
(
    id       serial NOT NULL PRIMARY KEY,
    city     text   NOT NULL,
    country  text   NOT NULL,
    email    text   NOT NULL UNIQUE,
    location text   NOT NULL,
    name     text   NOT NULL,
    phone    text   NOT NULL UNIQUE,
    state    text
);

create table transactions_fact
(
    id           serial           NOT NULL PRIMARY KEY,
    customer_id  integer          NOT NULL references customers_dim (id) ON DELETE CASCADE,
    date         date             NOT NULL,
    product_id   integer          NOT NULL references products_dim (id) ON DELETE CASCADE,
    quantity     integer          NOT NULL,
    seller_id    integer          NOT NULL references sellers_dim (id) ON DELETE CASCADE,
    store_id     integer          NOT NULL references stores_dim (id) ON DELETE CASCADE,
    total_price  double precision NOT NULL,
    pet_category text             NOT NULL,
    supplier_id  integer          NOT NULL references suppliers_dim (id) ON DELETE CASCADE
);
