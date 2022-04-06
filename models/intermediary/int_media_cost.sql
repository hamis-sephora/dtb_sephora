
{{ config(materialized='table') }}

-------- Consolidation Données Trafic -------
with base as (
SELECT
  Date,
  country,
  platform,
  case when channel in ('Paid Search Non Brand', 'Paid Search Brand') then ' Search'
       else channel end as channel,
  sum(sessions) as sessions ,
  sum(transactions) as transactions,
  round(sum(revenue),2) as revenue,
  --round(sum(revenue_euro),2) as revenue_euro
  FROM  {{ref('prd_all_trafic')}} 
  group by 1,2,3,4
) , 

base_data as ( 
select 
     Date, 
     concat(Date,'_',country,'_',platform, '_',channel) as id, 
     country , 
     platform ,
     channel , 
     sessions , 
     transactions, 
     revenue , 
     --revenue_euro
     from base
),

--------- Consolidation Social Data --------
social as ( 
  SELECT
  date,
  country, 
  'website' as platform,
  'Social' as channel,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  round(sum(final_costs),2) as final_costs
FROM  {{ref('stg_facebook')}} 
group by 1,2

union all 
SELECT 
  date,
  country, 
  'website' as platform,
  'Social' as channel,
  sum(impressions) as impressions,
  sum(story_completes) as clicks,
  round(sum(final_costs),2) as final_costs
   FROM {{ref('stg_snapchat')}} 
  group by 1, 2  
),

final_socials as (
select
   date, 
   country, 
   platform, 
   channel,
   concat(Date,'_',country,'_',platform, '_',channel) as id,    
   sum(impressions) as impressions,
   sum(clicks) as clicks,
   round(sum(final_costs),2) as final_costs
   from social 
   group by 1,2,3,4,5
) , 
adwords as (
SELECT 
  date,
  country, 
  'website' as platform,
  'Search' as channel,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  round(sum(final_costs),2) as final_costs
 FROM {{ref('stg_adwords')}} 
 group by 1,2
 ), 

adwords_final as (
SELECT 
  date,
  country, 
  platform,
  channel,
  concat(Date,'_',country,'_',platform, '_',channel) as id,   
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  round(sum(final_costs),2) as final_costs
 FROM adwords
 group by 1,2,3,4

)


select 
      base_data.Date, 
      base_data.country as country, 
      base_data.platform, 
      base_data.id, 
      base_data.channel , 
      base_data.sessions , 
      base_data.revenue , 
      --base_data.revenue_euro ,
      final_socials.final_costs as social_costs,
      adwords_final.final_costs as adwords_costs
    from base_data 
    left join final_socials
    on base_data.id = final_socials.id 
    left join adwords_final
    on base_data.id = adwords_final.id 

