{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'reporting'}  
  )
}}

with data_info as ( 
select 
    extract (week(sunday) from date ) as week_sunday_number, 
    extract (year from date) as year,
    country, 
    channel_grouping, 
    platform , 
    sum(sessions) as sessions,
    sum(sessions_retrieved) as sessions_retrieved , 
    sum(transactions) as transactions , 
    sum(revenue_local) as revenue_local, 
    sum(revenue_euro) as revenue_euro,     
    sum(addtocart) as session_addtocart
from {{ref('prd_all_trafic')}} 
group by 1,2,3,4,5
)
select 
    week_sunday_number, 
    country, 
    channel_grouping, 
    platform,
    sum (case when year=2021 then sessions end ) as sessions_2021,
    sum (case when year=2022 then sessions end ) as sessions_2022,
    sum (case when year=2021 then sessions_retrieved end) as sessions_retrieved_2021,
    sum (case when year=2022 then sessions_retrieved end) as sessions_retrieved_2022,
    sum (case when year=2021 then transactions end ) as transactions_2021,
    sum (case when year=2022 then transactions end) as transactions_2022,
    sum (case when year=2021 then revenue_euro end ) as revenue_local,
    sum (case when year=2022 then revenue_euro end ) as revenue_euro_2022,
    sum (case when year=2021 then revenue_local end ) as revenue_local_2021,
    sum (case when year=2022 then revenue_local end ) as revenue_local_2022,         
    sum (case when year=2021 then session_addtocart end ) as session_addtocart_2021,    
    sum (case when year=2022 then session_addtocart end ) as session_addtocart_2022
    from data_info
    group by 1,2,3,4

