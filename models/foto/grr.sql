{{ config(
    materialized="table"
) }}

with prev_year as (
    SELECT
      i.photographer_id AS pid,
      YEAR(i.invoice_date) AS revenue_year,
      -- sum(ii.total_net_eur) AS total
      SUM(CASE WHEN ii.item_type IN ('hosting', 'order_commission', 'printservice_production', 'printservice_shipping', 'photo_editing') THEN ii.total_net_eur ELSE 0 END) AS total,
      SUM(CASE WHEN ii.item_type = 'hosting' THEN ii.total_net_eur ELSE 0 END) AS subscription,
      SUM(CASE WHEN ii.item_type = 'order_commission' THEN ii.total_net_eur ELSE 0 END) AS order_commission,
      SUM(CASE WHEN ii.item_type IN ('printservice_production', 'printservice_shipping') THEN ii.total_net_eur ELSE 0 END) AS photo_labs,
      SUM(CASE WHEN ii.item_type = 'photo_editing' THEN ii.total_net_eur ELSE 0 END) AS photo_editing
    FROM LOCAL.PUBLIC.invoices i
    LEFT JOIN LOCAL.PUBLIC.invoices_items ii ON i.id = ii.invoice_id
    WHERE ii.item_type IN ('hosting', 'order_commission', 'printservice_production', 'printservice_shipping', 'photo_editing')
      -- AND YEAR(i.invoice_date) < 2022
      AND i.photographer_id IS NOT NULL
    GROUP BY
      i.photographer_id,
      YEAR(i.invoice_date)
    ORDER BY
      i.photographer_id,
      YEAR(i.invoice_date)
),


became_churn as (
        SELECT
          plcsc.photographer_id,
          YEAR(plcsc.transition_date) AS year,
          MAX(plcsc.transition_date) AS date,
          1 AS users
        FROM
          LOCAL.PUBLIC.photographers_stage_changes plcsc
        WHERE
          plcsc.new_stage_id = 5
        GROUP BY
          plcsc.photographer_id,
          YEAR(plcsc.transition_date)
        ORDER BY
          plcsc.photographer_id,
          YEAR(plcsc.transition_date)
),


became_new as (
        SELECT
          plcsc.photographer_id,
          YEAR(plcsc.transition_date) AS year,
          MAX(plcsc.transition_date) AS date,
          1 AS users
        FROM
          LOCAL.PUBLIC.photographers_stage_changes plcsc
        WHERE
          plcsc.new_stage_id = 2
        GROUP BY
          plcsc.photographer_id,
          YEAR(plcsc.transition_date)
        ORDER BY
          plcsc.photographer_id,
          YEAR(plcsc.transition_date)
),


calculate_type as (
    select distinct
        plcsm.photographer_id,
        (YEAR(plcsm.monthend_date)-1) as revenue_year,
        CASE 
            WHEN (became_churn.users = 1 AND became_churn.date > IFNULL(became_new.date,'0')) THEN 'lost'
            WHEN (became_new.users = 1 AND became_new.date > IFNULL(became_churn.date,'0')) THEN
                CASE 
                    WHEN became_new.year = YEAR(plcs.first_became_customer) THEN 'new_first_time'
                    ELSE 'new_reactivation'
                END
            WHEN (IFNULL(SUM(CASE WHEN ii.item_type IN ('hosting','order_commission','printservice_production','printservice_shipping','photo_editing') THEN ii.total_net_eur ELSE 0 END),0)-IFNULL(prev_year.total,0)) < 0 THEN 'downsell'
            ELSE 'upsell'
        END as user_status,
        IFNULL(prev_year.total,0) as prev_total
    from LOCAL.PUBLIC.photographers_per_month plcsm
    inner JOIN LOCAL.PUBLIC.invoices i
        ON i.photographer_id = plcsm.photographer_id 
        AND TO_CHAR(i.invoice_date, 'MM-YYYY') = TO_CHAR(plcsm.monthend_date, 'MM-YYYY')
    LEFT JOIN LOCAL.PUBLIC.invoices_items ii 
        ON i.id = ii.invoice_id
    LEFT JOIN LOCAL.PUBLIC.photographers_current plcs 
        ON plcsm.photographer_id = plcs.photographer_id
    LEFT JOIN prev_year
        ON plcsm.photographer_id = prev_year.pid and (YEAR(plcsm.monthend_date)-1) = prev_year.revenue_year
    LEFT JOIN became_new ON became_new.year = YEAR(plcsm.monthend_date) AND became_new.photographer_id = plcsm.photographer_id
    LEFT JOIN  became_churn ON became_churn.year = YEAR(plcsm.monthend_date) AND became_churn.photographer_id = plcsm.photographer_id
    group by 
        plcsm.photographer_id,
        -- i.photographer_id,
        --prev_year.revenue_year,
        YEAR(plcsm.monthend_date),
        revenue_year,
        became_churn.users,
        became_churn.date,
        became_new.users,
        became_new.date,
        became_new.year,
        plcs.first_became_customer,
        IFNULL(prev_year.total,0)
),


grr as (
    SELECT
        p.id as pid,
        year(p.created) as registration_year,
        case when p.country IN ('DE','AT','CH') then 'DACH' when p.country = 'GB' then 'UK' when p.country IN ('US','CA') then 'US' else 'RoW' end as registration_region,
        case when p.country IN ('DE', 'AT', 'CH', 'GB') then 'EU' when p.country IN ('US', 'CA') then 'US' else 'RoW' end as registration_region_2, 
        case when p.company_size IN ('s','m') then 'SMB' when p.company_size = 'ent' then 'Enterprise' else 'Starters' end as registration_size,
        YEAR(plcsm.monthend_date) as revenue_year,
        p.currency as shop_currency,
        p.billing_currency,
        type.user_status,
        SUM(CASE WHEN ii.item_type IN ('hosting', 'order_commission', 'printservice_production', 'printservice_shipping', 'photo_editing') THEN ii.total_net_eur ELSE 0 END) AS total,
        type.prev_total
    FROM LOCAL.PUBLIC.photographers_per_month plcsm
    LEFT JOIN LOCAL.PUBLIC.invoices i 
        ON i.photographer_id = plcsm.photographer_id 
        AND TO_CHAR(i.invoice_date, 'MM-YYYY') = TO_CHAR(plcsm.monthend_date, 'MM-YYYY')
    LEFT JOIN LOCAL.PUBLIC.invoices_items ii 
        ON i.id = ii.invoice_id
    LEFT JOIN LOCAL.PUBLIC.photographers p 
        ON plcsm.photographer_id = p.id
    LEFT JOIN LOCAL.PUBLIC.photographers_current plcs 
        ON plcsm.photographer_id = plcs.photographer_id
    LEFT JOIN calculate_type type
        ON p.id = type.photographer_id
        and (YEAR(plcsm.monthend_date)-1) = type.revenue_year
    GROUP BY
        p.id,
        year(p.created),
        case when p.country IN ('DE','AT','CH') then 'DACH' when p.country = 'GB' then 'UK' when p.country IN ('US','CA') then 'US' else 'RoW' end,
        case when p.country IN ('DE', 'AT', 'CH', 'GB') then 'EU' when p.country IN ('US', 'CA') then 'US' else 'RoW' end,
        case when p.company_size IN ('s','m') then 'SMB' when p.company_size = 'ent' then 'Enterprise' else 'Starters' end,
        YEAR(plcsm.monthend_date),
        p.currency,
        p.billing_currency,
        type.user_status,
        type.prev_total
)


select *, concat(pid, '_', revenue_year) as id from grr
order by 
        pid,
        registration_year,
        registration_region,
        registration_region_2, 
        registration_size,
        revenue_year,
        shop_currency,
        billing_currency,
        user_status