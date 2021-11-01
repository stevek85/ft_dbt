

  create or replace table `ft-data-330317`.`ft_data`.`my_second_dbt_model`
  
  
  OPTIONS()
  as (
    -- Use the `ref` function to select from other models


select *
from `ft-data-330317`.`ft_data`.`account_logs`
where customer_id = 1539
  );
    