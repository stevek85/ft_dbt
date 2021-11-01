{% macro create_udfs() %}

create schema if not exists {{target.schema}};

{{create_tumble_interval()}};

{{create_timeseries_index()}};

{% endmacro %}