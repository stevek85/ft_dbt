

create schema if not exists ft_data;


    CREATE OR REPLACE FUNCTION ft_data.tumble_interval(
     val TIMESTAMP, tumble_seconds INT64)
    AS (
     timestamp_seconds(div(UNIX_SECONDS(val), tumble_seconds) *  tumble_seconds))
;


    CREATE OR REPLACE FUNCTION
     ft_data.timeseries_index(keys ARRAY<INT64>, tumble_seconds INT64, min_ts TIMESTAMP, max_ts Timestamp)
    AS ((
     SELECT ARRAY_AGG(x)
     FROM (
       SELECT series_key, tumble_val
       FROM UNNEST(
         GENERATE_TIMESTAMP_ARRAY(
           ft_data.tumble_interval(min_ts, tumble_seconds),
           ft_data.tumble_interval(max_ts, tumble_seconds),
           INTERVAL tumble_seconds SECOND
         )
       ) AS tumble_val
       CROSS JOIN UNNEST(keys) AS series_key
     ) x
    ))
;

