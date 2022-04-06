{{ config(materialized='table') }}


with facebook_data as (
SELECT
  cast(substr(date, 0 , 10) as date) as date,
  extract ( week(sunday) from cast(substr(date, 0 , 10) as date) ) as week_number, 
  extract ( month from cast(substr(date, 0 , 10) as date) ) as month,
  extract ( year from cast(substr(date, 0 , 10) as date) ) as year, 
  concat ( extract ( year from cast(substr(date, 0 , 10) as date) ),'_',extract ( month from cast(substr(date, 0 , 10) as date) )) as year_month, 
  account_id,
  account_name,
  account_currency,
  campaign_name,
  sum(impressions) as impressions,
  sum(inline_link_clicks) as clicks,
  sum(reach) as reach, 
  round(sum(spend),2) as costs
FROM
  {{ source('cost_data', 'facebook_data') }} 
group by 1,2,3,4,5,6,7 ,8,9
),

orga_account as (
  SELECT
  account_id,
  keep,
  country,
  account_name
FROM {{ source('cost_data', 'orga_fcbk') }} 
),
exchange_data as (
SELECT
  year_month,
  From_Currency,
  To_Currency,
  inverse_rate
  from   {{ source('cost_data', 'exchange_data') }} 
), 
facebook_exchange as (
select 
      date, 
      week_number, 
      month, 
      year, 
      facebook_data.year_month , 
      facebook_data.account_id,
      facebook_data.account_name , 
      keep, 
      country,
      account_currency ,
      campaign_name,
      impressions,
      clicks ,
      costs ,
      To_Currency, 
      inverse_rate

 from facebook_data
 left join exchange_data
 on facebook_data.account_currency = exchange_data.To_Currency and facebook_data.year_month = exchange_data.year_month
 left join orga_account
 on facebook_data.account_id = orga_account.account_id
) ,
consolidation as (
select 
       date, 
       week_number, 
       month, 
       year, 
       year_month,
       account_id,
       account_name,
       keep, 
       country,       
       account_currency, 
       campaign_name,
       impressions ,
       clicks, 
       costs,
       case when To_Currency is null then "EUR" else To_Currency end as currency,  
       case when inverse_rate is null then 1 else inverse_rate end as inverse_rate, 
       from facebook_exchange 
)
select 
      date, 
      week_number, 
      month, 
      year,
      year_month, 
      account_id, 
      account_name, 
      keep, 
      country,      
      account_currency, 
      campaign_name,
      impressions, 
      clicks, 
      costs, 
      currency, 
      inverse_rate, 
      round ((costs*inverse_rate),2) as final_costs
      from consolidation
      order by date desc 
      








