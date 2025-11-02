-- 创建数据库并切换
CREATE DATABASE IF NOT EXISTS olist_db;
USE olist_db;

-- 表结构定义
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS sellers;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS order_payments;
DROP TABLE IF EXISTS order_reviews;

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);

CREATE TABLE order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
);

CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

CREATE TABLE customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

CREATE TABLE order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2)
);

CREATE TABLE order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT
);

DROP TABLE IF EXISTS product_category_name_translation;
CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);

DROP TABLE IF EXISTS geolocation;
CREATE TABLE geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10)
);


-- 导入数据
LOAD DATA LOCAL INFILE '/Users/fangyifan/Desktop/STAT5206 pro/olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, customer_id, order_status,
 @purchase, @approved, @carrier, @customer, @estimated)
SET
 order_purchase_timestamp = NULLIF(NULLIF(@purchase, ''), '0000-00-00 00:00:00'),
 order_approved_at = NULLIF(NULLIF(@approved, ''), '0000-00-00 00:00:00'),
 order_delivered_carrier_date = NULLIF(NULLIF(@carrier, ''), '0000-00-00 00:00:00'),
 order_delivered_customer_date = NULLIF(NULLIF(@customer, ''), '0000-00-00 00:00:00'),
 order_estimated_delivery_date = NULLIF(NULLIF(@estimated, ''), '0000-00-00 00:00:00');

LOAD DATA LOCAL INFILE '/Users/fangyifan/Desktop/STAT5206 pro/olist_order_items_dataset.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/fangyifan/Desktop/STAT5206 pro/olist_products_dataset.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/fangyifan/Desktop/STAT5206 pro/olist_sellers_dataset.csv'
INTO TABLE sellers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/fangyifan/Desktop/STAT5206 pro/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/fangyifan/Desktop/STAT5206 pro/olist_order_payments_dataset.csv'
INTO TABLE order_payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/fangyifan/Desktop/STAT5206 pro/olist_order_reviews_dataset.csv'
INTO TABLE order_reviews
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/fangyifan/Desktop/STAT5206 pro/product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/fangyifan/Desktop/STAT5206 pro/olist_geolocation_dataset.csv'
INTO TABLE geolocation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- 创建增强整合表
DROP TABLE IF EXISTS olist_consolidated_full;

ALTER TABLE orders ADD INDEX idx_orders_order_id (order_id);
ALTER TABLE orders ADD INDEX idx_orders_customer_id (customer_id);
ALTER TABLE customers ADD INDEX idx_customers_customer_id (customer_id);
ALTER TABLE customers ADD INDEX idx_customers_zip (customer_zip_code_prefix);
ALTER TABLE order_items ADD INDEX idx_items_order_id (order_id);
ALTER TABLE order_items ADD INDEX idx_items_product_id (product_id);
ALTER TABLE order_items ADD INDEX idx_items_seller_id (seller_id);
ALTER TABLE products ADD INDEX idx_products_product_id (product_id);
ALTER TABLE products ADD INDEX idx_products_category (product_category_name);
ALTER TABLE sellers ADD INDEX idx_sellers_seller_id (seller_id);
ALTER TABLE sellers ADD INDEX idx_sellers_zip (seller_zip_code_prefix);
ALTER TABLE order_reviews ADD INDEX idx_reviews_order_id (order_id);
ALTER TABLE order_payments ADD INDEX idx_payments_order_id (order_id);
ALTER TABLE product_category_name_translation ADD INDEX idx_translation_category (product_category_name);
ALTER TABLE geolocation ADD INDEX idx_geolocation_zip_prefix (geolocation_zip_code_prefix);


CREATE TABLE olist_consolidated_full AS
SELECT DISTINCT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.shipping_limit_date,
    oi.price,
    oi.freight_value,

    pt.product_category_name_english,

    LOWER(REPLACE(REPLACE(REPLACE(p.product_category_name, 'ã', 'a'), 'ç', 'c'), 'é', 'e')) AS product_category_ascii,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    LOWER(REPLACE(REPLACE(REPLACE(REPLACE(s.seller_city, 'ã', 'a'), 'ç', 'c'), 'á', 'a'), 'ó', 'o')) AS seller_city_ascii,
    s.seller_state,
    s.seller_zip_code_prefix,

    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,

    pmt.payment_methods,
    pmt.max_installments,
    pmt.total_payment_value,

    r.avg_review_score,
    r.sample_title,
    r.sample_comment

FROM order_items AS oi
INNER JOIN orders AS o ON oi.order_id = o.order_id
INNER JOIN products AS p ON oi.product_id = p.product_id
INNER JOIN sellers AS s ON oi.seller_id = s.seller_id
INNER JOIN customers AS c ON o.customer_id = c.customer_id
LEFT JOIN (
    SELECT order_id,
           SUM(payment_value) AS total_payment_value,
           MAX(payment_installments) AS max_installments,
           GROUP_CONCAT(DISTINCT payment_type) AS payment_methods
    FROM order_payments
    GROUP BY order_id
) AS pmt ON o.order_id = pmt.order_id
LEFT JOIN (
    SELECT order_id,
           AVG(review_score) AS avg_review_score,
           MAX(review_comment_message) AS sample_comment,
           MAX(review_comment_title) AS sample_title
    FROM order_reviews
    GROUP BY order_id
) AS r ON o.order_id = r.order_id
LEFT JOIN product_category_name_translation AS pt
    ON p.product_category_name = pt.product_category_name


WHERE
    o.order_purchase_timestamp IS NOT NULL
    AND o.order_delivered_customer_date IS NOT NULL
    AND o.order_approved_at IS NOT NULL
    AND o.order_delivered_carrier_date IS NOT NULL
    AND o.order_estimated_delivery_date IS NOT NULL
    AND oi.price IS NOT NULL
    AND oi.freight_value IS NOT NULL
    AND p.product_category_name IS NOT NULL
    AND s.seller_city IS NOT NULL
    AND r.avg_review_score IS NOT NULL;

SELECT * FROM olist_consolidated_full;