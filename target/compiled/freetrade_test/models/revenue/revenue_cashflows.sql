

WITH isa_revenues AS
(
       SELECT customer_id,
              TIMESTAMP(billing_date)  AS transaction_datetime,
              amount,
              "isa_monthly_fee" AS transaction_type
       FROM   `ft-data-330317`.`ft_data`.`subscription_revenue_cashflows` where
       subscription_type = 'isa'),

sipp_revenues AS

(

       SELECT customer_id,
              TIMESTAMP(billing_date)  AS transaction_datetime,
              amount,
              "sipp_monthly_fee" AS transaction_type
       FROM   `ft-data-330317`.`ft_data`.`subscription_revenue_cashflows`
       where subscription_type = 'sipp'),

fx_revenues AS

(SELECT customer_id, datetime AS transaction_datetime, amount, 'US_fx_charge' as transaction_type
 FROM `ft-data-330317`.`ft_data`.`fx_revenue_cashflows`
)


       SELECT *
       FROM   isa_revenues
       UNION ALL
                 (
                    SELECT *
                        FROM   sipp_revenues )
          UNION ALL
                    (
                           SELECT *
                           FROM   fx_revenues)

             ORDER BY  customer_id,
                       transaction_datetime