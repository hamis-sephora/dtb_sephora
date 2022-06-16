{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'reporting'}  
   )
}}


select
    week, 
    country,  
    sum (case when extract(year from date) = 2021 then sessions else 0 end ) as sessions_2021,  
    sum (case when extract(year from date) = 2022 then sessions else 0 end ) as sessions_2022,
    sum (case when extract(year from date) = 2021 then sessions_retrieved else 0 end ) as sessions_retrieved_2021,  
    sum (case when extract(year from date) = 2022 then sessions_retrieved else 0 end ) as sessions_retrieved_2022,
    sum (case when extract(year from date) = 2021 then addtocart else 0 end ) as addtocart_2021,  
    sum (case when extract(year from date) = 2022 then addtocart else 0 end ) as addtocart_2022,    
    sum (case when extract(year from Date) = 2021 then sessions_forecast else 0 end ) as sessions_forecast_2021,  
    sum (case when extract(year from Date) = 2022 then sessions_forecast else 0 end ) as sessions_forecast_2022,   
    round(sum (case when extract(year from Date) = 2021 then sessions_cbs else 0 end ),2) as sessions_cbs_2021,  
    round(sum (case when extract(year from Date) = 2022 then sessions_cbs else 0 end ),2) as sessions_cbs_2022,      
    sum (case when extract(year from Date) = 2021 then transactions_cbs else 0 end ) as transactions_cbs_2021,  
    sum (case when extract(year from Date) = 2022 then transactions_cbs else 0 end ) as transactions_cbs_2022,     
    sum (case when extract(year from Date) = 2021 then revenue_euro_cbs else 0 end ) as revenue_euro_cbs_2021,  
    sum (case when extract(year from Date) = 2022 then revenue_euro_cbs else 0 end ) as revenue_euro_cbs_2022,            
    sum (case when extract(year from Date) = 2021 then turnover_forecast else 0 end ) as turnover_forecast_2021,  
    sum (case when extract(year from Date) = 2022 then turnover_forecast else 0 end ) as turnover_forecast_2022,   
FROM   
  {{ref('prd_session_d')}} 
  where week > 0 
group by 1,2  
ORDER by week asc , country asc 
