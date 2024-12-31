{{ config(materialized='table') }}

select 
    n_nationkey as nation_key,
    --this lookup table is only used for customers  
    n_name as customer_nation
    
from {{ source('sample', 'nation') }} 
