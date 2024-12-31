{{ config(materialized='table') }}

select 
	--keys
    o_orderkey as order_key,
    o_custkey as customer_key,

	--order details
    o_totalprice as order_total,
    o_orderdate as order_date,

	--truncate date to month for monthly reports
    date_trunc('month', o_orderdate) as order_month,

	--make statuses more readable
    case 
        when o_orderstatus = 'O' then 'Open'
        when o_orderstatus = 'F' then 'Filled'
        when o_orderstatus = 'P' then 'Processing'
        end as order_status

from {{ source('sample', 'orders') }}
