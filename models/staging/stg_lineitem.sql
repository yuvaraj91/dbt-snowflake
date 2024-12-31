{{ config(materialized='table') }}

select 
    concat(l_orderkey, '---', l_linenumber) as line_key,
    l_orderkey as order_key,
    l_quantity as line_quantity

from {{ source('sample', 'lineitem') }}
