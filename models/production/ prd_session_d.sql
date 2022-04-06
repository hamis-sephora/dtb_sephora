{{ config(materialized='table') }}


SELECT
Date , 
country, 
platform, 
sessions, 
conversions, 
round(revenue_local,2) as revenue_local, 
round(revenue_euros,2) as revenue_euros, 
IFNULL(sessions_rattrapee, 0) as sessions_rattrapee, 
case when IFNULL(sessions_rattrapee, 0) = 0 Then sessions else IFNULL(sessions_rattrapee, 0) end as sessions_retrieved, 
IFNULL(sessions_forecast, 0) as sessions_forecast ,
budget
FROM   
  {{ref('int_report_session_d')}} 
ORDER by Date desc , country asc 
