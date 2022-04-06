{{ config(materialized='table') }}


 with facebook as (
SELECT
  week_number,
  account_name,  
  'facebook' as media, 
  country,
  round(sum (case when year = 2021 then final_costs else 0 end ),2) as cost_2021,
  round(sum (case when year = 2022 then final_costs else 0 end ),2) as cost_2022
 FROM {{ref('stg_facebook')}} 
 where  week_number > 0 and keep = 'OUI'
 group by 1,2,3,4
 ),

 adwords as (
select 
   week_number,
   account_name,  
  'adwords' as media, 
  country,
  round(sum (case when year = 2021 then final_costs else 0 end ),2) as cost_2021,
  round(sum (case when year = 2022 then final_costs else 0 end ),2) as cost_2022
 FROM {{ref('stg_adwords')}} 
 where week_number > 0 and keep = 'OUI'
 group by 1,2,3,4 
 ), 
snapchat as (
 select 
   week_number,
   account_name,  
  'snapchat' as media, 
  country,
  round(sum (case when year = 2021 then final_costs else 0 end ),2) as cost_2021,
  round(sum (case when year = 2022 then final_costs else 0 end ),2) as cost_2022
 FROM {{ref('stg_snapchat')}} 
  where week_number > 0 and keep = 'OUI'
 group by 1,2,3,4
)

select * from facebook
union all 
select * from adwords 
union all 
select * from snapchat 
order by week_number asc 
