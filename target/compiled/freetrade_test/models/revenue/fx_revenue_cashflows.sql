

/* Assume here that FT take 1% charge on US orders */

SELECT customer_id, 0.01 * value as amount,
       order_id, settled_at as datetime FROM `ft-data-330317.ft_source_data.orders` where instrument_country = 'US'

ORDER BY datetime ASC