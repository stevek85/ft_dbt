

  create or replace table `ft-data-330317`.`ft_data`.`monthly_subscription_revenue`
  
  
  OPTIONS()
  as (
    

with months as (

/*
call as follows:

date_spine(
    "day",
    "to_date('01/01/2016', 'mm/dd/yyyy')",
    "dateadd(week, 1, current_date)"
)

*/

with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
    
    

    )

    select *
    from unioned
    where generated_number <= 12
    order by generated_number



),

all_periods as (

    select (
        

        datetime_add(
            cast( cast('2021-01-01' as date) as datetime),
        interval row_number() over (order by 1) - 1 month
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast('2022-01-01' as date)

)

select * from filtered

)

select
    subscriptions.customer_id,
    subscriptions.subscription_id,
    subscriptions.monthly_fee,
    subscriptions.subscription_type,
    subscriptions.created_at,
    DATE_ADD(months.date_month, INTERVAL (
        EXTRACT(DAY FROM subscriptions.created_at) - EXTRACT(DAY FROM months.date_month) - 15) DAY) as billing_date

from `ft-data-330317.ft_source_data.subscriptions` subscriptions

join months
    -- all months after start date
    on  months.date_month >= DATE(subscriptions.created_at)
    -- and before end date
    and months.date_month <= DATE('2022-01-01')

ORDER BY customer_id, date_month ASC
  );
    