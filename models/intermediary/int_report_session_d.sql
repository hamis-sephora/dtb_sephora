{{ config(materialized='table') }}


-- Consolidation des données site web uniquement 
with table_1 as (

SELECT 
     Date, 
     upper(country) as country, 
     'website' as platform, 
     'ga_global' as source, 
      sum (sessions) as sessions , 
      sum (conversions) as conversions, 
      sum (revenue_local) as revenue_local ,
      sum (revenue) as revenue_euros
FROM  {{ref('stg_ga_global')}} 
group by 1, 2 ,3 , 4

UNION ALL 
-- Consolidation du trafic NON APP RU uniquement

SELECT
    Date, 
    upper(country) as country, 
    'firebase_ru' as source,
    'website' as platform, 
    sum (sessions) as sessions , 
    sum (conversions) as conversions, 
    sum (revenue_local) as revenue_local ,
    sum (revenue) as revenue_euros ,         
  FROM {{ref('stg_not_app_global')}} 
  group by 1, 2, 3 , 4
  order by 1 asc 

) , 

-- Consolidation des données de sessions redressées

table_2 as ( 

SELECT Date , 
       country, 
       'website' as platform, 
       sum(sessions) as sessions, 
       sum(sessions_rattrapee) as sessions_rattrapee
 FROM {{ref('stg_sessions_retrieved_d')}} 
 group by 1,2,3
), 

 -- Consolidation données forecast

 table_3 as (
SELECT
    Date, 
    country,
    sum(sessions_forecast) as sessions_forecast,
    sum(budget) as budget
FROM
   {{ref('stg_forecast_d')}}  
   group by 1,2
)

-- Consolidation des données site web et sessions redressées
select 
      table_1.Date as Date,
      table_1.country as country, 
      table_1.source, 
      'website' as platform , 
      table_1.sessions,
      table_1.conversions,
      table_1.revenue_local,
      table_1.revenue_euros, 
      table_2.sessions_rattrapee,
      table_3.sessions_forecast,
      table_3.budget
      from table_1
      left join table_2
      on concat(table_1.Date,'_',table_1.country) = concat(table_2.Date,'_',table_2.country)
      left join table_3
      on concat(table_1.Date,'_',table_1.country) = concat(table_3.Date,'_',table_3.country)

UNION ALL 

-- Consolidation du trafic App
SELECT
    Date, 
    upper(country) as country, 
    'cybersource_app' as source, 
    'App' as platform, 
    sum (sessions) as sessions , 
    sum (conversions) as conversions, 
    sum (revenue_local) as revenue_local ,
    sum (revenue) as revenue_euros ,         
    0 as sessions_rattrapee_2022 , 
    0 as sessions_forecast,
    0 as budget
  FROM  {{ref('stg_app_global')}} 
  group by 1, 2, 3 , 4 









