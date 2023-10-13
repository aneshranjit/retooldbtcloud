{{
    config(
        materialized='table'
    )
}}

WITH shopify_sales1 AS (
    SELECT
        cast(null as STRING) as common_salon_name,
        shipping_address.city as city,
        shipping_address.province_code as province,
        null as salon_id,
        cast(id as STRING) as transaction_id,
        (if(cancelled_at is null, if(financial_status = "paid" and fulfillment_status = "fulfilled", "Completed", "Pending"), "Cancelled")) as transaction_status,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S',  substr(created_at, 0, 19)) as purchase_date_time, 
        shopify_orders.customer.email as customer_email,
        shopify_orders.customer.first_name as customer_first_name,
        shopify_orders.customer.last_name as customer_last_name,
        line_items.name as item_name,
        cast((select 
          prod_norm.category
        from `ellabache-singleview.retool_db.shortcutsent_product_normalisation` as prod_norm
        where prod_norm.shortcutsent_product_name = line_items.name
        group by prod_norm.category) as STRING) as item_category,
        "Product" as item_type,
        cast(line_items.quantity as NUMERIC) as item_qty,
        cast(line_items.pre_tax_price as FLOAT64) as item_amount,
        "Yes" as is_ellabache_product,
        "Shopify" as data_source
    FROM `ellabache-singleview.retool_db.shopify_orders` as shopify_orders
    LEFT OUTER JOIN `ellabache-singleview.retool_db.shopify_customers` as shopify_customers ON shopify_orders.customer_id = shopify_customers.id
    INNER JOIN `ellabache-singleview.retool_db.shopify_order_lineitems` as line_items ON shopify_orders.id = line_items.order_id
)

SELECT 
    common_salon_name,
    city,
    province,
    salon_id,
    transaction_id,
    transaction_status,
    purchase_date_time, 
    customer_email,
    customer_first_name,
    customer_last_name,
    item_name,
    item_category,
    item_type,
    item_qty,
    item_amount,
    is_ellabache_product,
    data_source
FROM 
    shopify_sales1
order by shopify_sales1.purchase_date_time desc
