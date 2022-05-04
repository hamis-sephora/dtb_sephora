

{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}
select  
      Date , 
      country, 
      sum(case when Data_Source_type='adwords' then cost end) as cost_adwords, 
      sum(case when Data_Source_type='awin' then cost end) as cost_awin, 
      sum(case when Data_Source_type='criteo' then cost end) as cost_criteo, 
      sum(case when Data_Source_type='facebookads' then cost end) as cost_facebookads, 
      sum(case when Data_Source_type='rtbhouse' then cost end) as cost_rtbhouse, 
      sum(case when Data_Source_type='snapchat' then cost end) as cost_snapchat, 
      sum(case when Data_Source_type='tiktok' then cost end) as cost_tiktok,
      sum(cost) as cost
from {{ref('stg_funnel_global_data')}} 
group by 1,2