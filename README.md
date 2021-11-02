
## Freetrade data modelling exercise

I spent a fair amount of time getting to know dbt and plugging it into google cloud so i could experiment with queries on the fly. Disclaimer - i'm not particularly knowledgable on the do's and don'ts of dbt - but i've modelled the data in a way i believe would be useful for analytics.


## Models

### [Account Cashflows](https://github.com/stevek85/ft_dbt/blob/f1484908270c86d3078dff485d8a5be967be5410/models/accounts/account_cashflows.sql)

This is simply a union of deposits, withdrawals, buys and sells. The idea here is that if you have a table of all cashflow "deltas" on an account - you can then construct a timeseries of the withdrawable account balance on any customer account. (see next model)

### [Withdrawable cash timeseries](https://github.com/stevek85/ft_dbt/blob/f1484908270c86d3078dff485d8a5be967be5410/models/accounts/withdrawable_cash_timeseries.sql)

1. "tumble" the cashflow dates into discrete time bins (every 6hrs) and for each customer,  aggregate the cashflows in each bin. This uses a tumble UDF defined in the macros directory
2.  Create a timeseries index per customer by performing an outer join between a timeseries index (spaced every 6hrs and between the min max tumble dates of 1. - i used a timeseries index macro for this), and the aggregated events table in 1. The outer join is keyed on customer_id and tumble date, and null values of cashflow deltas are colaesced to 0.
3. The table resulting from 2 is an aggregated timeseries of cashflow deltas per customer. To get the withdrawable account balance for each customer, perform a cum sum partitioned customer on ascending tumble dates)

![](https://github.com/stevek85/ft_dbt/blob/f1c457c26357bf98d55e0c84bece554e88111ecd/Screenshot%202021-11-02%20at%2020.24.20.png)

### [FX revenue cashflows](https://github.com/stevek85/ft_dbt/blob/f1484908270c86d3078dff485d8a5be967be5410/models/revenue/fx_revenue_cashflows.sql)
To create an fx revenue table,  I made the assumption the ft charge 1% of buy/sell orders on US stocks. The SQL is trivial

### [Subscription evenue cashflows](https://github.com/stevek85/ft_dbt/blob/f1484908270c86d3078dff485d8a5be967be5410/models/revenue/subscription_revenue_cashflows.sql)
Idea here was to build a table of subscription revenue events - using the subscription tables as a base to generate monthly payment events. I've assumed all subscriptions are continuous through 2021 and that the first subscription payment is 1 month after subscription start date
1. Used date spine dbt util to create a set of dates (1st of month - interval month), spanning 2021.
2. Then joined subscription table onto 1. filtering out rows where the date index is less than the subcription start date (otherwise you're counting subscription payments for customers before they signed up)

### [Revenue cashflows](https://github.com/stevek85/ft_dbt/blob/f1484908270c86d3078dff485d8a5be967be5410/models/revenue/revenue_cashflows.sql)

Union of fx revenue cashflows and subscription_revenue_cashflows to provide an easy way to look at revenue over time broken down by source.

![](https://github.com/stevek85/ft_dbt/blob/b18692e0f03eeffc79db7d49ac83cc18372c69fa/Screenshot%202021-11-02%20at%2020.19.04.png)









