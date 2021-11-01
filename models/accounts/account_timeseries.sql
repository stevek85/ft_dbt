/*
A timeseries of account balances for each client binned every 4hrs.
Can definitely be tidied up - e.g. not use inline functions but need to figure out how to do this in dbt
*/

{{ config(materialized='table') }}

/*
Project/aggregate cashflow data into tumbled bins and project onto customer_id, datetime index
*/

    WITH event_logs AS
    (
             SELECT   * ,
                      sum(amount) OVER ( partition BY customer_id ORDER BY log_datetime rows BETWEEN UNBOUNDED PRECEDING AND      CURRENT row) AS balance
             FROM     {{ ref('account_cashflows') }}
             ORDER BY log_datetime ASC),

          agged_events AS
    (
             SELECT   sum(amount) delta,
                      customer_id,
                     {{target.schema}}.tumble_interval(log_datetime, 21600) tumble
             FROM     event_logs
             GROUP BY customer_id,
                      tumble
             ORDER BY customer_id,
                      tumble ASC ),
           args AS
    (
           SELECT array_agg(DISTINCT customer_id) AS KEY,
                  min(tumble)                     AS min_ts,
                  max(tumble)                     AS max_ts
           FROM   agged_events ),

           timeseries AS
    (
                    SELECT          series_key         AS customer_id,
                                    tumble_val         AS tumble,
                                    COALESCE(delta, 0) AS def,
                                    delta              AS unfilled
                    FROM            unnest(
                                    (
                                           SELECT  {{target.schema}}.timeseries_index(KEY, 21600, min_ts, max_ts)
                                           FROM   args) ) a
                    LEFT OUTER JOIN agged_events b
                    ON              a.series_key = b.customer_id
                    AND             a.tumble_val = b.tumble
                    ORDER BY        customer_id,
                                    tumble_val DESC)

    SELECT   customer_id,
             tumble                                                                                                           AS datetime,
             sum(def) OVER ( partition BY customer_id ORDER BY tumble rows BETWEEN UNBOUNDED PRECEDING AND      CURRENT row ) AS account_balance
    FROM     timeseries
    ORDER BY customer_id,
             tumble ASC