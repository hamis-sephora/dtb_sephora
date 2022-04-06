{{ config(materialized='table') }}


with snpachat_data as (
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
  sum(shares) as shares,
  sum(saves) as saves,
  sum(story_completes) as story_completes,
  sum(spend) as costs
  from {{ source('cost_data', 'snpachat_data') }} 
group by 1,2,3,4,5,6,7,8,9  
) , 

exchange_data as ( 
SELECT
  year_month,
  From_Currency,
  To_Currency,
  inverse_rate
   from  {{ source('cost_data', 'exchange_data') }} 
),
orga_account as (
SELECT
  name,
  status,
  country,
  keep
  FROM {{ source('cost_data', 'orga_snap') }} 
),
snapchat_exchange as (
select 
      date, 
      week_number, 
      month, 
      year, 
      snpachat_data.year_month , 
      account_id,
      account_name , 
      account_currency ,
      campaign_name,
      country,
      keep,
      impressions,
      shares ,
      saves ,
      costs,
      story_completes,
      To_Currency, 
      inverse_rate

 from snpachat_data
 left join exchange_data
 on snpachat_data.account_currency = exchange_data.To_Currency and snpachat_data.year_month = exchange_data.year_month
 left join orga_account 
 on snpachat_data.account_name = orga_account.name
), 

consolidation as (
select 
       date, 
       week_number, 
       month, 
       year, 
       year_month,
       account_id,
       account_name,
       account_currency, 
       campaign_name,
       country,
       keep,       
       impressions,
       shares ,
       saves ,
       story_completes,
       costs,
       case when To_Currency is null then "EUR" else To_Currency end as currency,  
       case when inverse_rate is null then 1 else inverse_rate end as inverse_rate, 
       from snapchat_exchange 
)
select 
      date, 
      week_number, 
      month, 
      year,
      year_month, 
      account_id, 
      account_name, 
      account_currency, 
      campaign_name,
      country,
      keep,      
      impressions,
      shares ,
      saves ,
      story_completes,
      costs,
      currency, 
      inverse_rate, 
      round ((costs*inverse_rate),2) as final_costs
      from consolidation

