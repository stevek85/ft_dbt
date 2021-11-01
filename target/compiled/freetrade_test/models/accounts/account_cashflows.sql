/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



WITH deposits AS
(
       SELECT customer_id,
              deposited_at  AS log_datetime,
              amount_in_gbp AS amount,
              "deposit"     AS transaction_type
       FROM   `ft-data-330317.ft_source_data.deposits`), withdrawals AS
(
       SELECT customer_id,
              requested_at AS log_datetime,
              -amount      AS amount,
              "withdrawal" AS transaction_type
       FROM   `ft-data-330317.ft_source_data.withdrawals`),

       buys AS
(
       SELECT customer_id,
              executed_at AS log_datetime,
              -value      AS amount,
              "buy"       AS transaction_type,
       FROM   `ft-data-330317.ft_source_data.orders`
       WHERE  side = 'buy'),

       sells AS
(
       SELECT customer_id,
              executed_at AS log_datetime,
              value       AS amount,
              "sell"      AS transaction_type,
       FROM   `ft-data-330317.ft_source_data.orders`
       WHERE  side = 'sell'),

       cashflows AS
(
       SELECT *
       FROM   withdrawals
       UNION ALL
                 (
                        SELECT *
                        FROM   deposits)
          UNION ALL
                    (
                           SELECT *
                           FROM   buys)
             UNION ALL
                       (
                              SELECT *
                              FROM   sells)
             ORDER BY  customer_id,
                       log_datetime)
SELECT   *
FROM     cashflows
ORDER BY customer_id, log_datetime ASC

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null