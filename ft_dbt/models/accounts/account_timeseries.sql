/*
A timeseries of account balances for each client binned every 4hrs.
Can definitely be tidied up - e.g. not use inline functions but need to figure out how to do this in dbt
*/


{{ config(materialized='table') }}

/*
 inline function to "tumble" datetimes into date bins
*/


    CREATE TEMP FUNCTION tumble_interval(
     val TIMESTAMP, tumble_seconds INT64)
    AS (
     timestamp_seconds(div(UNIX_SECONDS(val), tumble_seconds) *  tumble_seconds));

/*
inline function to create the customer_id, datetime index onto which account data will be projected
*/

    CREATE TEMP FUNCTION
     gen_ts_candidates(keys ARRAY<INT64>, tumble_seconds INT64, min_ts TIMESTAMP, max_ts Timestamp)
    AS ((
     SELECT ARRAY_AGG(x)
     FROM (
       SELECT series_key, tumble_val
       FROM UNNEST(
         GENERATE_TIMESTAMP_ARRAY(
           tumble_interval(min_ts, tumble_seconds),
           tumble_interval(max_ts, tumble_seconds),
           INTERVAL tumble_seconds SECOND
         )
       ) AS tumble_val
       CROSS JOIN UNNEST(keys) AS series_key
     ) x
    ));

/*
Project/aggregate cashflow data into tumbled bins and project onto customer_id, datetime index
*/

    WITH event_logs AS
    (
             SELECT   * ,
                      sum(amount) OVER ( partition BY customer_id ORDER BY log_datetime rows BETWEEN UNBOUNDED PRECEDING AND      CURRENT row) AS balance
             FROM     {{ ref('cashflows') }}
             ORDER BY log_datetime ASC),

          agged_events AS
    (
             SELECT   sum(amount) delta,
                      customer_id,
                      tumble_interval(log_datetime, 21600) tumble
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
                                           SELECT gen_ts_candidates(KEY, 21600, min_ts, max_ts)
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