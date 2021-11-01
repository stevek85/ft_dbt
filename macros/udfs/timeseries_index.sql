/*
create the customer_id, datetime index onto which account data will be projected
*/

{% macro create_timeseries_index() %}
    CREATE OR REPLACE FUNCTION
     {{target.schema}}.timeseries_index(keys ARRAY<INT64>, tumble_seconds INT64, min_ts TIMESTAMP, max_ts Timestamp)
    AS ((
     SELECT ARRAY_AGG(x)
     FROM (
       SELECT series_key, tumble_val
       FROM UNNEST(
         GENERATE_TIMESTAMP_ARRAY(
           {{target.schema}}.tumble_interval(min_ts, tumble_seconds),
           {{target.schema}}.tumble_interval(max_ts, tumble_seconds),
           INTERVAL tumble_seconds SECOND
         )
       ) AS tumble_val
       CROSS JOIN UNNEST(keys) AS series_key
     ) x
    ))
{% endmacro %}