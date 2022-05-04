{{
  config(
    materialized = 'table',
   labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'int'}  )
}}

with table_0 as (
select 
      concat(date,'_',country,'_', datasource_cs) as ligne_id, 
      date, 
      country, 
      datasource_cs as platform,
      'cybersource' as source, 
     sum(sessions) as sessions, 
     sum(transactions) as transactions,
     sum(revenue_local) as revenue_local, 
     sum(revenue_euro) as revenue_euro
from  {{ref('stg_global_cybersource')}} 
group by 1,2,3,4,5
order by date desc 
),
table_1 as (
select 
     concat(Date,'_',upper(country),'_','website') as ligne_id, 
     Date, 
     upper(country) as country, 
     'website' as platform, 
     'ga_global' as source, 
      sum(sessions) as sessions , 
      sum(addtocart) as addtocart, 
      sum(searches) as searches, 
      sum(list_scroll) as list_scroll, 
      sum(skincare) as skincare, 
      sum(fragrance) as fragrance, 
      sum(makeup) as makeup, 
      sum(hair) as hair, 
      sum(conversions) as conversions, 
      sum(revenue_local) as revenue_local ,
      sum(revenue) as revenue_euros
from {{ref('stg_ga_consolidation')}} 
group by 1, 2 ,3 , 4 ,5
),
table_2 as ( 
select 
       concat (Date,'_',country,'_','website') as ligne_id,
       Date , 
       country, 
       'website' as platform, 
       sum(sessions) as sessions, 
       sum(sessions_rattrapee) as sessions_rattrapee
 FROM {{ref('stg_sessions_retrieved_d')}} 
 group by 1,2,3,4
), 

 table_3 as (
select 
    concat(Date,'_',country,'_','website') as ligne_id,
    Date, 
    country,
    sum(sessions_forecast) as sessions_forecast,
    sum(budget) as budget
FROM
   {{ref('stg_forecast_d')}}  
   group by 1,2,3
)

select
/* Données cybersource */
      table_0.ligne_id,
      table_0.date, 
      table_0.country, 
      table_0.platform, 
      table_0.sessions,
      table_0.transactions,
      table_0.revenue_euro,
      table_0.revenue_local, 
/* Données google analytics */ 
      table_1.sessions as sessions_ga,
      table_1.addtocart as addtocart_ga, 
      table_1.searches as searches_ga, 
      table_1.list_scroll as list_scroll_ga, 
      table_1.skincare as skincare_ga, 
      table_1.fragrance as fragrance_ga, 
      table_1.makeup as makeup_ga, 
      table_1.hair as hair_ga , 
      table_1.conversions as conversions_ga,
      table_1.revenue_local as revenue_local_ga,
      table_1.revenue_euros as revenue_euros_ga,
/* Données sessions redressées */      
      table_2.sessions_rattrapee,
/* Données sessions forecast et budget */      
      table_3.sessions_forecast,
      table_3.budget
      from table_0
      left join table_1
      on table_0.ligne_id = table_1.ligne_id
      left join table_2
      on table_0.ligne_id = table_2.ligne_id
      left join table_3
      on table_0.ligne_id = table_3.ligne_id
order by table_0.date desc 








