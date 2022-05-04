{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

select * 

from {{ref('stg_ga_consolidation')}} 
where channel = 'Emailing'