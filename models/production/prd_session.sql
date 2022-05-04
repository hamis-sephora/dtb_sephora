{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'reporting'}  
   )
}}


SELECT
  week,
  country,
  platform,
  sessions_2021 as trafic_2021,
  sessions_2022 as trafic_2022,
  addtocart_2021, 
  addtocart_2022,
  skincare_2021,
  skincare_2022,
  fragrance_2021, 
  fragrance_2022, 
  makeup_2021, 
  makeup_2022, 
  hair_2021, 
  hair_2022, 
  sessions_rattrapee_2022, 
  sessions_forecast as trafic_forecast, 
  case when sessions_rattrapee_2022 = 0 
  or sessions_rattrapee_2022 is null then sessions_2022 
  else sessions_rattrapee_2022 end as trafic_retrieved_2022,
  conversions_2021, 
  conversions_2022,
  revenue_2021, 
  revenue_2022
FROM   
  {{ref('int_report_session')}} 
  where week > 0
ORDER by week asc , country asc 
