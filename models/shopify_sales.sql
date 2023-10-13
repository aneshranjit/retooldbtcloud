{{
    config(
        materialized='table'
    )
}}

with shopify_sales1 as (
    select 
        cast(null as STRING) as common_salon_name,
        shipping_address.city as city,
        shipping_address.province_code as province,
        null as salon_id,
        cast(id as STRING) as transaction_id,
        (if(cancelled_at is null, if(financial_status = "paid" and fulfillment_status = "fulfilled", "Completed", "Pending"), "Cancelled")) as transaction_status,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S',  substr(created_at, 0, 19)) as sale_datetime, 
        shopify_orders.customer.email as customer_email,
        shopify_orders.customer.first_name as customer_first_name,
        shopify_orders.customer.last_name as customer_last_name,
        line_items 
    from 
        `ellabache-singleview.ellabacheshopifyorders_clone.ebdb_shopify_orders` as shopify_orders,
    UNNEST(line_items) line_items
)

select 
    common_salon_name,
    city,
    province,
    salon_id,
    transaction_id,
    transaction_status,
    sale_datetime as purchase_date_time, 
    customer_email,
    customer_first_name,
    customer_last_name,
    line_items.value.name as item_name,
    cast((select 
        prod_norm.category
    from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_product_normalisation` as prod_norm
    where prod_norm.shortcutsent_product_name = line_items.value.name
    group by prod_norm.category) as STRING) as item_category,
    "Product" as item_type,
    cast(line_items.value.quantity as NUMERIC) as item_qty,
    (cast(line_items.value.quantity as NUMERIC) * cast(line_items.value.pre_tax_price as FLOAT64)) as item_amount,
    "Yes" as is_ellabache_product,
    "Shopify" as data_source
from 
    shopify_sales1
order by shopify_sales1.sale_datetime desc
