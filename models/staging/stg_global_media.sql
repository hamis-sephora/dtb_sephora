
{{ config(materialized='table') }}

with data as (

SELECT
  date,
  week_number,
  month,
  year,
  country, 
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  round(sum(final_costs),2) as final_costs
FROM  {{ref('stg_facebook')}}
  where keep='OUI'
  group by 1,2,3,4,5
  
  UNION ALL

  SELECT
  date,
  week_number,
  month,
  year,
  country,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  round(sum(final_costs),2) as final_costs
FROM {{ref('stg_adwords')}}
  where keep='OUI'
  group by 1,2,3,4,5

  UNION ALL 

 SELECT
  date,
  week_number,
  month,
  year,
  country,
  sum(impressions) as impressions,
  sum(shares) as clicks,
  round(sum(final_costs),2) as final_costs
FROM {{ref('stg_snapchat')}}
  where keep='OUI'
  group by 1,2,3,4,5

)

select 
    date, 
    week_number, 
    month,
    year, 
    country,
    sum(impressions) as impressions ,
    sum(clicks) as clicks ,
    round(sum(final_costs),2) as final_costs
    from data
    group by 1,2,3,4,5