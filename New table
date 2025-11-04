SELECT DISTINCT
    o.order_id,
    o.customer_id,
    o.order_status,
    TIMESTAMPDIFF(HOUR, o.order_purchase_timestamp, o.order_delivered_customer_date) AS "商品实际送达耗费时间",
    TIMESTAMPDIFF(HOUR, o.order_purchase_timestamp, oi.shipping_limit_date) AS "商品限制配送耗费时间",
    TIMESTAMPDIFF(HOUR, o.order_purchase_timestamp, o.order_estimated_delivery_date) AS "物流预计送达耗费时间",
    TIMESTAMPDIFF(HOUR, o.order_delivered_customer_date, o.order_estimated_delivery_date) AS "实际商品提前送达的天数",

    oi.order_item_id,
    oi.product_id,
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
