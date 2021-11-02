
{{ config(materialized='table') }}

with months as ({{ dbt_utils.date_spine(
    datepart="month",
    start_date="cast('2021-01-01' as date)",
    end_date="cast('2022-01-01' as date)",
   )
}})

select
    subscriptions.customer_id,
    subscriptions.subscription_id,
    subscriptions.monthly_fee as amount,
    subscriptions.subscription_type,
    DATE_ADD(months.date_month, INTERVAL (
        EXTRACT(DAY FROM subscriptions.created_at) - EXTRACT(DAY FROM months.date_month) - 15) DAY) as billing_date

from `ft-data-330317.ft_source_data.subscriptions` subscriptions

join months
    -- all months after start date
    on  months.date_month >= DATE(subscriptions.created_at)
    -- and before end date
    and months.date_month <= DATE('2022-01-01')

ORDER BY customer_id, date_month ASC

