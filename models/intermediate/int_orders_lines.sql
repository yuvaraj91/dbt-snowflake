{{ config(materialized='table') }}

select 
    o.order_key,

    --line_key and order_key are both unique and non-null 
    --so we don't need a distinct count for unique_items
    count(1) as order_unique_items,
    sum(line_quantity) as order_total_quantity

from {{ ref('stg_orders') }} as o 

    inner join {{ ref('stg_lineitem') }} as l 
        on l.order_key = o.order_key 

group by o.order_key
