{{
  config(
    materialized = 'table',
    labels = {'type': 'piano_analytics', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

select 
      date, 
      'FR' as country, 
      count(distinct concat (TO_HEX(VISITOR_ID),'_',TO_HEX(VISIT_ID)) ) as visites,
      count(distinct transaction_id) as transactions, 
   from 
     {{ source('piano', 'raw_export_partitioned') }} 
  group by 1   