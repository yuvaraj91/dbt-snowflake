{{ config(materialized='table') }}

with orders as (

select 
    o.order_key,
    o.order_status,
    o.order_total,
    o.order_date,
    o.order_month,
    l.order_unique_items,
    l.order_total_quantity,
    c.customer_key,
    c.customer_name,
    n.customer_nation
    
from {{ ref('stg_orders') }} as o 
    
    inner join {{ ref('int_orders_lines') }} l 
        on l.order_key = o.order_key 

    inner join {{ ref('stg_customer') }} c 
        on c.customer_key = o.customer_key

    inner join {{ ref('stg_nation') }} as n 
        on n.nation_key = c.nation_key
)

select * from orders
