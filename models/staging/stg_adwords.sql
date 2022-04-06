{{ config(materialized='table') }}

with adwords_data as ( 
SELECT
  cast(substr(date, 0 , 10) as date) as date,
  extract ( week(sunday) from cast(substr(date, 0 , 10) as date) ) as week_number, 
  extract ( month from cast(substr(date, 0 , 10) as date) ) as month,
  extract ( year from cast(substr(date, 0 , 10) as date) ) as year, 
  concat ( extract ( year from cast(substr(date, 0 , 10) as date) ),'_',extract ( month from cast(substr(date, 0 , 10) as date) )) as year_month, 
  customer_id,
  account_name,
  currency_code,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  round(sum(costs),2) as costs 
from {{ source('cost_data', 'adwords_data') }} 
group by 1,2,3,4,5,6,7,8
), 

orga_account as (
  SELECT
  name,
  keep,
  country,
  category
FROM {{ source('cost_data', 'orga_adwords') }} 
),

exchange_data as ( 
SELECT
  year_month,
  From_Currency,
  To_Currency,
  inverse_rate
  from   {{ source('cost_data', 'exchange_data') }} 
) , 
adwords_exchange as (
select 
      date, 
      week_number, 
      month, 
      year, 
      adwords_data.year_month , 
      customer_id , 
      account_name , 
      currency_code ,
      country,
      keep, 
      category,
      impressions,
      clicks ,
      costs ,
      To_Currency, 
      inverse_rate

 from adwords_data
 left join exchange_data
 on adwords_data.currency_code = exchange_data.To_Currency and adwords_data.year_month = exchange_data.year_month
 left join orga_account 
 on adwords_data.account_name = orga_account.name
) ,
consolidation as (
select 
       date, 
       week_number, 
       month, 
       year, 
       year_month,
       customer_id,
       account_name,
       currency_code, 
       country,
       keep,
       category,       
       impressions ,
       clicks, 
       costs,
       case when To_Currency is null then "EUR" else To_Currency end as currency,  
       case when inverse_rate is null then 1 else inverse_rate end as inverse_rate, 
       from adwords_exchange 
)
select 
      date, 
      week_number, 
      month, 
      year,
      year_month, 
      customer_id, 
      account_name, 
      currency_code, 
      country,
      keep,
      category,      
      impressions, 
      clicks, 
      costs, 
      currency, 
      inverse_rate, 
      round ((costs*inverse_rate),2) as final_costs
      from consolidation




























