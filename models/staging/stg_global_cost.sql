{{ config(materialized='table') }}


with social as ( 
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

select * from final_socials
union all 
select * from adwords_final