/*
 inline function to "tumble" datetimes into date bins
*/

{% macro create_tumble_interval() %}
    CREATE OR REPLACE FUNCTION {{target.schema}}.tumble_interval(
     val TIMESTAMP, tumble_seconds INT64)
    AS (
     timestamp_seconds(div(UNIX_SECONDS(val), tumble_seconds) *  tumble_seconds))
{% endmacro %}