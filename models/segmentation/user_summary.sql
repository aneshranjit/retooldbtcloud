{{
    config(
    materialized='table',
    cluster_by = ['customer_identifier'],
    )
}}

with customers as (
    select 
c.customer_identifier, 
c.purchase_date, 
c.revenue,
c.orders,
last_purchase.last_purchase_date,

from {{ ref('agg_customer_details') }} c
inner join (
    select 
    customer_identifier,
    max(purchase_date) as last_purchase_date,
    from {{ ref('agg_customer_details') }}
    group by 1
) last_purchase on last_purchase.customer_identifier = c.customer_identifier

)

select 
customer_identifier, 
timestamp_diff(current_datetime(), last_purchase_date, day) as recency,
sum(orders) as total_orders,
sum(revenue) as total_revenue,
from customers
group by 1,2

