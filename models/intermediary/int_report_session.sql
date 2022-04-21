{{ config(materialized='table') }}


-- Consolidation des données site web uniquement 
with table_1 as (

SELECT 
     extract (week (sunday) from Date) as week_number, 
     upper(country) as country, 
     'website' as platform, 
    sum (case when extract(year from Date)  = 2021 then sessions else 0 end ) as sessions_2021,  
    sum (case when extract(year from Date)  = 2022 then sessions else 0 end ) as sessions_2022, 
    sum (case when extract(year from Date)  = 2021 then addtocart else 0 end ) as addtocart_2021,  
    sum (case when extract(year from Date)  = 2022 then addtocart else 0 end ) as addtocart_2022,  
    sum (case when extract(year from Date)  = 2021 then skincare else 0 end ) as skincare_2021,  
    sum (case when extract(year from Date)  = 2022 then skincare else 0 end ) as skincare_2022, 
    sum (case when extract(year from Date)  = 2021 then fragrance else 0 end ) as fragrance_2021,  
    sum (case when extract(year from Date)  = 2022 then fragrance else 0 end ) as fragrance_2022,     
    sum (case when extract(year from Date)  = 2021 then makeup else 0 end ) as makeup_2021,  
    sum (case when extract(year from Date)  = 2022 then makeup else 0 end ) as makeup_2022,     
    sum (case when extract(year from Date)  = 2021 then hair else 0 end ) as hair_2021,  
    sum (case when extract(year from Date)  = 2022 then hair else 0 end ) as hair_2022,                               
    sum (case when extract(year from Date)  = 2021 then conversions else 0 end ) as conversions_2021,  
    sum (case when extract(year from Date)  = 2022 then conversions else 0 end ) as conversions_2022,   
    round(sum (case when extract(year from Date)  = 2021 then revenue_local else 0 end ),2) as revenue_2021,  
    round(sum (case when extract(year from Date)  = 2022 then revenue_local else 0 end ),2) as revenue_2022                 
FROM  {{ref('stg_ga_global')}} 
group by 1, 2 ,3

) , 

-- Consolidation des données de sessions redressées

table_2 as ( 

SELECT week_number , 
       country, 
       'website' as platform, 
       sum (case when year  = '2022' then sessions else 0 end ) as sessions_2022, 
       sum (case when year  = '2022' then sessions_rattrapee else 0 end ) as sessions_rattrapee_2022
 FROM {{ref('stg_sessions_retrieved')}} 
 group by 1,2,3
), 

 -- Consolidation données forecast

 table_3 as (
SELECT
    week_number, 
    country,
    sessions_forecast
FROM
   {{ref('stg_forecast')}}  
)
-- Consolidation des données site web et sessions redressées
select 
      table_1.week_number as week,
      table_1.country as country, 
      'website' as platform , 
      table_1.sessions_2021,
      table_1.sessions_2022 ,  
      table_1.addtocart_2021,
      table_1.addtocart_2022,
      table_1.skincare_2021, 
      table_1.skincare_2022,
      table_1.fragrance_2021,
      table_1.fragrance_2022, 
      table_1.makeup_2021, 
      table_1.makeup_2022, 
      table_1.hair_2021,
      table_1.hair_2022,      
      table_1.conversions_2021 ,       
      table_1.conversions_2022 ,       
      table_1.revenue_2021 ,       
      table_1.revenue_2022 ,            
      table_2.sessions_rattrapee_2022 ,
      table_3.sessions_forecast
      from table_1
      left join table_2
      on concat(table_1.week_number,'_',table_1.country) = concat(table_2.week_number,'_',table_2.country)
      left join table_3
      on concat(table_1.week_number,'_',table_1.country) = concat(table_3.week_number,'_',table_3.country)

UNION ALL 

-- Consolidation du trafic App
SELECT
    extract (week (sunday) from date) as week_number, 
    upper(country) as country, 
    'App' as platform, 
    sum (case when extract(year from Date)  = 2021 then sessions else 0 end ) as sessions_2021,  
    sum (case when extract(year from Date)  = 2022 then sessions else 0 end ) as sessions_2022,
    0 as addtocart_2021, 
    0 as addtocart_2022, 
    0 as skincare_2021, 
    0 as skincare_2022, 
    0 as fragrance_2021,
    0 as fragrance_2022, 
    0 as makeup_2021, 
    0 as makeup_2022, 
    0 as hair_2021, 
    0 as hair_2022,  
    sum (case when extract(year from Date)  = 2021 then conversions else 0 end ) as conversions_2021,  
    sum (case when extract(year from Date)  = 2022 then conversions else 0 end ) as conversions_2022,   
    round(sum (case when extract(year from Date)  = 2021 then revenue else 0 end ),2) as revenue_2021,  
    round(sum (case when extract(year from Date)  = 2022 then revenue else 0 end ),2) as revenue_2022,           
    0 as sessions_rattrapee_2022 , 
    0 as sessions_forecast,
  FROM  {{ref('stg_app_global')}} 
  group by 1, 2, 3


UNION ALL 

-- Consolidation du trafic non App pour la Russie
SELECT
    extract (week (sunday) from date) as week_number, 
    upper(country) as country, 
    'App' as platform, 
    sum (case when extract(year from Date)  = 2021 then sessions else 0 end ) as sessions_2021,  
    sum (case when extract(year from Date)  = 2022 then sessions else 0 end ) as sessions_2022, 
    0 as addtocart_2021, 
    0 as addtocart_2022, 
    0 as skincare_2021, 
    0 as skincare_2022, 
    0 as fragrance_2021,
    0 as fragrance_2022, 
    0 as makeup_2021, 
    0 as makeup_2022, 
    0 as hair_2021, 
    0 as hair_2022,      
    sum (case when extract(year from Date)  = 2021 then conversions else 0 end ) as conversions_2021,  
    sum (case when extract(year from Date)  = 2022 then conversions else 0 end ) as conversions_2022,   
    round(sum (case when extract(year from Date)  = 2021 then revenue else 0 end ),2) as revenue_2021,  
    round(sum (case when extract(year from Date)  = 2022 then revenue else 0 end ),2) as revenue_2022,           
    0 as sessions_rattrapee_2022 , 
    0 as sessions_forecast
  FROM  {{ref('stg_not_app_global')}} 
  group by 1, 2, 3









