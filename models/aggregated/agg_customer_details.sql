{{
    config(
    materialized='table',
    partition_by={
      "field": "purchase_date",
      "data_type": "date"
    },
    cluster_by = ['customer_identifier'],
    )
}}

with customer_type as (
    select 
    customer_identifier,
    min(purchase_date) as first_order_date,
    from (
    select distinct
    coalesce(customer_email, concat(customer_first_name, customer_last_name)) as customer_identifier,
    purchase_date
    from {{ ref('total_sales') }}
    )
    where customer_identifier not in ('#WALKIN', '', 'WalkIn', '#WALK IN', 'Professional Product#cubicle')
    group by 1 
)

select 
t.purchase_date,
t.customer_identifier,
coalesce(t.salon_state, "N/A") as state,
t.data_source,
t.orders,
t.revenue,
case when customer_type.first_order_date = t.purchase_date then 'New Customer' else 'Returning Customer' end as customer_type,
sum(revenue) over(partition by t.customer_identifier, data_source) as source_wise_total_revenue,
sum(revenue) over(partition by t.customer_identifier) as total_revenue,
from
(select 
purchase_date,
coalesce(customer_email, concat(customer_first_name, customer_last_name)) as customer_identifier,
case when data_source = 'Shopify' then 'Ecommerce' else 'Salon' end as data_source,
salon_state,
count(distinct case when transaction_status = 'Completed' then transaction_id else null end) as orders,
sum(item_amount) as revenue,
from {{ ref('total_sales') }}
group by 1,2,3,4
) t 
inner join customer_type on customer_type.customer_identifier = t.customer_identifier 
where t.customer_identifier not in ('#WALKIN', '', 'WalkIn', '#WALK IN', 'Professional Product#cubicle')
