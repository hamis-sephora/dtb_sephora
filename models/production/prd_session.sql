{{ config(materialized='table') }}


SELECT
  week,
  country,
  platform,
  sessions_2021 as trafic_2021,
  sessions_2022 as trafic_2022,
  sessions_rattrapee_2022, 
  sessions_forecast as trafic_forecast, 
  case when sessions_rattrapee_2022 = 0 
  or sessions_rattrapee_2022 is null then sessions_2022 
  else sessions_rattrapee_2022 end as trafic_retrieved_2022,
FROM   
  {{ref('int_report_session')}} 
  where week > 0
ORDER by week asc , country asc 
