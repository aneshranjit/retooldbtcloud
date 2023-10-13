{{ config(
    materialized='incremental',
    partition_by={
      "field": "purchase_date",
      "data_type": "date"
    },
    cluster_by = ['item_category', 'item_name'],
    incremental_strategy = 'insert_overwrite',
    )
}}

with sales as (
    select
        common_salon_name,
        salon_name,
        salon_state,
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
    from {{ ref('enterprise_sales') }}
    union all
    select
        common_salon_name,
        salon_name,
        salon_state,
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
    from {{ ref('pos_sales') }}
    union all 
    select
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
    from {{ ref('shopify_sales') }}
),

total_sales as (
    select
        date(purchase_date_time) as purchase_date,
        purchase_date_time,
        common_salon_name,
        salon_name,
        salon_state,
        salon_id,
        transaction_id,
        transaction_status,
        customer_email,
        customer_first_name,
        customer_last_name,
        item_name,
        item_category,
        replace(item_type, 'ItemType.', '') as item_type,
        cast(item_qty as NUMERIC) as item_qty,
        cast(item_amount as FLOAT64) as item_amount,
        is_ellabache_product,
        data_source,
        row_number() over(partition by transaction_id,item_name 
                        order by item_amount desc) rn
    from sales
)


select *except(rn) from total_sales
{% if is_incremental() %}
    where date(purchase_date_time) >= '2022-01-01'
{% else %}
    where date(purchase_date_time) >= '2018-01-01'
{% endif %}
and (case when rn != 1 and item_amount = 0.0 then 1 else null end) is null
