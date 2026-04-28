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
    pet_id      integer references customer_pets_dim (id)
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
    customer_id  integer          NOT NULL references customers_dim (id),
    date         date             NOT NULL,
    product_id   integer          NOT NULL references products_dim (id),
    quantity     integer          NOT NULL,
    seller_id    integer          NOT NULL references sellers_dim (id),
    store_id     integer          NOT NULL references stores_dim (id),
    total_price  double precision NOT NULL,
    pet_category text             NOT NULL,
    supplier_id  integer          NOT NULL references suppliers_dim (id)
);


