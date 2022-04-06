{{ config(materialized='table') }}

with data as (
SELECT
  extract( week(sunday) from PARSE_DATE('%Y%m%d',date) ) week_number,
  extract( year from PARSE_DATE('%Y%m%d',date) ) year,
  country,
  ROUND(SUM(CAST(sessions_forecast AS float64)),2) AS sessions_forecast, 
  ROUND(SUM(CAST(Budget AS float64)),2) AS budget, 

FROM  {{ source('forecast', 'forecast_data') }} 

GROUP BY 1,2,3 
)
select * from data 
where year = 2022 and week_number > 0
order by week_number asc 