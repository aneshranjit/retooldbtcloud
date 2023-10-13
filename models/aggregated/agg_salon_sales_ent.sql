{{
    config(
    materialized='incremental',
    partition_by={
      "field": "purchase_date",
      "data_type": "date"
    },
    cluster_by = ['item_category', 'item_name'],
    incremental_strategy = 'insert_overwrite',
    )
}}

select 
purchase_date,
common_salon_name,
salon_name,
salon_state,
transaction_status,
item_name,
item_category,
item_type,
is_ellabache_product,
count(distinct transaction_id) as orders,
count(distinct coalesce(customer_email, concat(customer_first_name, customer_last_name))) as customers,
count(distinct case when item_type = 'Service' and transaction_status = 'Cancelled' then transaction_id else null end) as no_shows,
sum(item_qty) as quantity,
sum(item_amount) as revenue,
from {{ ref('total_sales') }}
where data_source in ('Shortcuts Enterprise')
{% if is_incremental() %}
    and purchase_date >= '2022-01-01'
{% else %}
    and purchase_date >= '2018-01-01'
{% endif %}
group by 1,2,3,4,5,6,7,8,9