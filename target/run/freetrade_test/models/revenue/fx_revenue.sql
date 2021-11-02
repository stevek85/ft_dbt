

  create or replace table `ft-data-330317`.`ft_data`.`fx_revenue`
  
  
  OPTIONS()
  as (
    

/* Assume here that FT take 1% charge on US orders */

SELECT customer_id, 0.01 * value as amount, value, side,
       order_id, settled_at as datetime FROM `ft-data-330317.ft_source_data.orders` where instrument_country = 'US'
ORDER BY datetime ASC
  );
    