{{
    config(
    materialized='table',
    cluster_by = ['item_name'],
    )
}}

with customer_orders as (
    select 
    *,
    row_number() over(partition by customer_identifier, item_type order by purchase_date asc) as rn 
    from
    (select 
    purchase_date,
    coalesce(customer_email, concat(customer_first_name, customer_last_name)) as customer_identifier,
    transaction_id,
    item_type, 
    item_name, 
    item_amount, 
    item_qty,
    from {{ ref('total_sales') }} ) t 
    where customer_identifier not in ('#WALKIN', '', 'WalkIn', '#WALK IN', 'Professional Product#cubicle')
    and item_amount >= 0.0
)

select 
*,
row_number() over(partition by item_type order by first_purchases desc) as first_purchases_rank,
row_number() over(partition by item_type order by second_purchases desc) as second_purchases_rank
from
(select 
item_name, 
item_type,
count(distinct case when rn = 1 then transaction_id else null end) as first_purchases,
count(distinct case when rn = 2 then transaction_id else null end) as second_purchases,
count(distinct case when rn > 1 then transaction_id else null end) as repeat_purchases,
count(distinct transaction_id) as total_purchases,
from customer_orders 
group by 1,2)