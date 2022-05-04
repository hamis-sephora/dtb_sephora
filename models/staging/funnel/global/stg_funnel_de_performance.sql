{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}
select  
      Date , 
      country, 
      Data_Source_type as regie_source, 
      Campaign, 
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost
from {{ref('stg_funnel_global_data')}} 
group by 1,2,3,4