{{ config(materialized='table') }}

with data as (
SELECT
  extract( week(sunday) from PARSE_DATE('%Y%m%d',date) ) week_number,
  extract( year from PARSE_DATE('%Y%m%d',date) ) year,
  case when country in ('UAE', 'QA', 'KW' , 'BH' , 'KSA' ,'OM' ) then 'ME'
            when country in ( 'IDB' , 'RU') then 'RU'
  else country end as country,   
  ROUND(SUM(CAST(sessions_forecast AS float64)),2) AS sessions_forecast, 
  ROUND(SUM(CAST(_turnoverforecast_ AS float64)),2) as _turnoverforecast_,
  ROUND(SUM(CAST(Orders_forecast AS float64)),2) as Orders_forecast,
  ROUND(SUM(CAST(Budget AS float64)),2) AS budget, 
FROM  {{ source('forecast', 'forecast_data') }} 

GROUP BY 1,2,3 
)
select week_number , 
       year ,
       country,
       sessions_forecast , 
       _turnoverforecast_, 
       Orders_forecast,
       budget
       from data
       where year = 2022 and week_number > 0
       order by week_number asc 