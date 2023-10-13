{{
    config(
        materialized='table'
    )
}}

WITH ent_sales1 as (
    select
        cast((select
            common_salons.normalised_salon_name
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcuts_common_salons` as common_salons
        where common_salons.ent_salon_id = ent_lineitems.siteid) as STRING) as common_salon_name,
        cast((select
            salon_norm.normalised_salon_name
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_salon_name_normalisation` as salon_norm
        where salon_norm.salon_id = ent_lineitems.siteid
        group by salon_norm.normalised_salon_name) as STRING) as salon_name,
        cast((select
            salon_norm.state
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_salon_name_normalisation` as salon_norm
        where salon_norm.salon_id = ent_lineitems.siteid
        group by salon_norm.state) as STRING) as salon_state,
        ent_lineitems.siteid as salon_id,
        cast(transactionid as STRING) as transaction_id,
        (case voidstatusstringcode
            when "VoidStatus.Normal" then "Completed"
            else "Cancelled"
        end) as transaction_status,
        transactiondatetime as purchase_date_time,
        cast((select
            ent_cus.emailaddress
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_customers` as ent_cus
        where ent_cus.customerid = ent_lineitems.clientid) as STRING) as customer_email,
        clientfirstname as customer_first_name,
        clientsurname as customer_last_name,
        itemname as item_name,
        itemtypestringcode as item_type,
        cast(itemquantity as NUMERIC) as item_qty,
        cast(lineextaxamount as FLOAT64) as item_amount,
        (case
            when se_prod.xlproductid is null then "No"
            else "Yes"
        end) as is_ellabache_product,
        "Shortcuts Enterprise" as data_source
    from 
        `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_transactionlines` as ent_lineitems
    left outer join `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_products` as se_prod on ent_lineitems.itemid = se_prod.productid
    where ent_lineitems.itemtypestringcode = "ItemType.Product"
    order by ent_lineitems.transactiondatetime desc
),
ent_sales2 as (
    select
        cast((select
            common_salons.normalised_salon_name
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcuts_common_salons` as common_salons
        where common_salons.ent_salon_id = ent_lineitems.siteid) as STRING) as common_salon_name,
        cast((select
            salon_norm.normalised_salon_name
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_salon_name_normalisation` as salon_norm
        where salon_norm.salon_id = ent_lineitems.siteid
        group by salon_norm.normalised_salon_name) as STRING) as salon_name,
        cast((select
            salon_norm.state
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_salon_name_normalisation` as salon_norm
        where salon_norm.salon_id = ent_lineitems.siteid
        group by salon_norm.state) as STRING) as salon_state,
        ent_lineitems.siteid as salon_id,
        cast(transactionid as STRING) as transaction_id,
        (case voidstatusstringcode
            when "VoidStatus.Normal" then "Completed"
            else "Cancelled"
        end) as transaction_status,
        transactiondatetime as purchase_date_time,
        cast((select
            ent_cus.emailaddress
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_customers` as ent_cus
        where ent_cus.customerid = ent_lineitems.clientid) as STRING) as customer_email,
        clientfirstname as customer_first_name,
        clientsurname as customer_last_name,
        itemname as item_name,
        itemtypestringcode as item_type,
        cast(itemquantity as NUMERIC) as item_qty,
        cast(lineextaxamount as FLOAT64) as item_amount,
        "Service" as is_ellabache_product,
        "Shortcuts Enterprise" as data_source
    from 
        `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_transactionlines` as ent_lineitems
    where itemtypestringcode = "ItemType.Service"
    order by ent_lineitems.transactiondatetime desc
),
ent_sales3 as (
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
        cast((select 
            prod_norm.normalised_product_name
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_product_normalisation` as prod_norm
        where prod_norm.shortcutsent_product_name = ent_sales1.item_name
        group by prod_norm.normalised_product_name) as STRING) as item_name,
        cast((select 
            prod_norm.category
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_product_normalisation` as prod_norm
        where prod_norm.shortcutsent_product_name = ent_sales1.item_name
        group by prod_norm.category) as STRING) as item_category,
        item_type,
        item_qty,
        item_amount,
        is_ellabache_product,
        data_source
    from 
        ent_sales1
    order by purchase_date_time desc
),
ent_sales41 as (
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
        (select any_value(cats) as item_category from (select
            serv_norm.category
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_service_normalisation` as serv_norm
        where serv_norm.shortcutsent_service_name = ent_sales2.item_name
        group by serv_norm.category) as cats) as item_category,
        item_type,
        item_qty,
        item_amount,
        is_ellabache_product,
        data_source
    from 
        ent_sales2
    order by purchase_date_time desc
),
ent_sales4 as (
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
        cast(ent_sales41.item_category.category as STRING) as item_category,
        item_type,
        item_qty,
        item_amount,
        is_ellabache_product,
        data_source
    from 
        ent_sales41
    order by purchase_date_time desc
),
ent_sales5 as (
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
    from 
        ent_sales3
    where item_name is not null
    order by purchase_date_time desc
),
ent_sales6 as (
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
    from 
        ent_sales5
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
    from 
        ent_sales4
)

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
from 
    ent_sales6
where item_qty >= 0    
order by purchase_date_time desc

