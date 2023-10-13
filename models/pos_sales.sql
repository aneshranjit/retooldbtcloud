{{
    config(
        materialized='table'
    )
}}

with pos_sales1 as (
    select 
        cast((select
            DISTINCT common_salons.normalised_salon_name
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcuts_common_salons` as common_salons
        where common_salons.pos_salon_id = pos_grpsales.site_id) as STRING) as common_salon_name,
        cast((select
            salon_norm.normalised_salon_name
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_salon_name_normalisation` as salon_norm
        where salon_norm.salon_id = pos_grpsales.site_id
        group by salon_norm.normalised_salon_name) as STRING) as salon_name,
        cast((select
            salon_norm.state
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_salon_name_normalisation` as salon_norm
        where salon_norm.salon_id = pos_grpsales.site_id
        group by salon_norm.state) as STRING) as salon_state,
        concat(sale_number, "_", site_id) as transaction_id,
        (case sale_status_code
            when "Completed" then "Completed"
            else "Cancelled"
        end) as transaction_status,
        sale_number,
        site_id,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S',  sale_date_time) as sale_datetime, 
        document_count,
        documents as docs
    FROM 
        `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutspos_groupsales` as pos_grpsales,
    UNNEST(documents) documents
),
pos_sales2 AS (
    SELECT 
        common_salon_name,
        salon_name,
        salon_state,
        transaction_id,
        transaction_status,
        sale_number,
        site_id,
        sale_datetime, 
        docs.value.customer.email as customer_email,
        docs.value.customer.first_name as customer_first_name,
        docs.value.customer.last_name as customer_last_name,
        document_count,
        docs.value.document_lines as line_items,
        array_length(docs.value.document_lines) as lineitems_len
    FROM pos_sales1
),
pos_sales3 AS (
    SELECT 
        common_salon_name,
        salon_name,
        salon_state,
        transaction_id,
        transaction_status,
        sale_number,
        site_id,
        sale_datetime, 
        customer_email,
        customer_first_name,
        customer_last_name,
        document_count,
        line_items,
        lineitems_len
    FROM pos_sales2,
    UNNEST(line_items) line_items
),
pos_sales4 as (
    select
        common_salon_name,
        salon_name,
        salon_state,
        site_id as salon_id,
        transaction_id,
        transaction_status,
        sale_datetime as purchase_date_time,
        customer_email,
        customer_first_name,
        customer_last_name,
        line_items.value.item.item_name as item_name,
        line_items.value.item.item_type_code as item_type,
        cast(line_items.value.quantity as NUMERIC) as item_qty,
        cast(line_items.value.total_ex_tax_amount as FLOAT64) as item_amount,
        "Yes" as is_ellabache_product,
        "Shortcuts POS" as data_source
    from pos_sales3
    where line_items.value.item.item_type_code = "Product"
    order by pos_sales3.sale_datetime desc
),
pos_sales5 as (
    select
        common_salon_name,
        salon_name,
        salon_state,
        site_id as salon_id,
        transaction_id,
        transaction_status,
        sale_datetime as purchase_date_time,
        customer_email,
        customer_first_name,
        customer_last_name,
        line_items.value.item.item_name as item_name,
        line_items.value.item.item_type_code as item_type,
        cast(line_items.value.quantity as NUMERIC) as item_qty,
        cast(line_items.value.total_ex_tax_amount as FLOAT64) as item_amount,
        "Service" as is_ellabache_product,
        "Shortcuts POS" as data_source
    from pos_sales3
    where line_items.value.item.item_type_code = "Service"
    order by pos_sales3.sale_datetime desc
),
pos_sales6 as (
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
        where prod_norm.shortcutsent_product_name = pos_sales4.item_name
        group by prod_norm.normalised_product_name) as STRING) as item_name,
        cast((select 
            prod_norm.category
        from `ellabache-singleview.ellabachesingleview_clone.ebdb_shortcutsent_product_normalisation` as prod_norm
        where prod_norm.shortcutsent_product_name = pos_sales4.item_name
        group by prod_norm.category) as STRING) as item_category,
        item_type,
        item_qty,
        item_amount,
        is_ellabache_product,
        data_source
    from 
        pos_sales4
    order by purchase_date_time desc
),
pos_sales71 as (
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
        where serv_norm.shortcutsent_service_name = pos_sales5.item_name
        group by serv_norm.category) as cats) as item_category,
        item_type,
        item_qty,
        item_amount,
        is_ellabache_product,
        data_source
    from 
        pos_sales5
    order by purchase_date_time desc
),
pos_sales7 as (
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
        cast(pos_sales71.item_category.category as STRING) as item_category,
        item_type,
        item_qty,
        item_amount,
        is_ellabache_product,
        data_source
    from 
        pos_sales71
    order by purchase_date_time desc
),
pos_sales8 as (
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
        pos_sales6
    where item_name is not null
    order by purchase_date_time desc
),
pos_sales9 as (
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
        pos_sales8
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
        pos_sales7
),
pos_sales_not_cmn as(
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
        pos_sales9
    where common_salon_name is null
),
pos_sales_is_cmn as(
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
        data_source,
        (if((select any_value(ent_cmn.purchase_date_time) from (select ent_sales.purchase_date_time from `ellabache-singleview.ellabachesingleview.all_ent_sales` as ent_sales where ent_sales.common_salon_name = pos_sales9.common_salon_name and ent_sales.purchase_date_time = pos_sales9.purchase_date_time) as ent_cmn) is null, false, true)) as is_dup,
    from pos_sales9
    where common_salon_name is not null
),
pos_sales_is_cmn_not_dup as(
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
        data_source,
        is_dup
    from pos_sales_is_cmn
    where is_dup = false
),
pos_sales_not_dup as(
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
    from pos_sales_not_cmn
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
    from pos_sales_is_cmn_not_dup
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
    pos_sales_not_dup
    where item_qty >= 0
    order by purchase_date_time desc

