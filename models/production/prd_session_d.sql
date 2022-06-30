{{
  config(
    materialized = 'table',
    labels = {'type': 'all_data', 'contains_pie': 'no', 'category':'reporting'}  
   )
}}

 with trafic_data as (
select 
       date , 
       week, 
       country, 
       concat(date,'_',country) as id, 
       sum(sessions) as sessions, 
       sum(sessions_retrieved) as sessions_retrieved, 
       sum(sessions_rattrapee) as sessions_rattrapee, 
       sum(addtocart) as addtocart, 
       sum(transactions) as transactions, 
       sum(revenue_local) as revenue_local, 
       sum(revenue_euro) as revenue_euro
from {{ref('prd_all_trafic')}} 
group by 1,2,3,4
 ) , 

 cybersource_data as ( 
  select 
    date,
    country,
    concat(date,'_',country) as id ,
    sum(sessions) as sessions_cbs,
    sum(transactions) as transactions_cbs,
    sum(revenue_local) as revenue_local_cbs,
    sum(revenue_euro) as revenue_euro_cbs
  from {{ref('stg_global_cybersource')}} 
group by 1,2
 ),

 forecast_data as ( 

   select 
        Date as date,
        country as country, 
        concat(date,'_',country) as id ,
        sum(sessions_forecast) as sessions_forecast, 
        sum(_turnoverforecast_ )as turnover_forecast, 
        sum(Orders_forecast) as Orders_forecast,
        sum(budget) as budget
 from {{ref('stg_forecast_d')}} 
      group by 1,2,3
 ), 

global_data as (
 select 
       trafic_data.date,
       trafic_data.week, 
       trafic_data.country,
       trafic_data.sessions, 
       trafic_data.sessions_retrieved,
       trafic_data.sessions_rattrapee,
       trafic_data.addtocart,
       trafic_data.transactions, 
       trafic_data.revenue_local,
       trafic_data.revenue_euro,
       forecast_data.sessions_forecast, 
       forecast_data.turnover_forecast, 
       forecast_data.Orders_forecast,
       cybersource_data.sessions_cbs,
       cybersource_data.transactions_cbs,
       cybersource_data.revenue_euro_cbs

       from trafic_data
      left join forecast_data
      on trafic_data.id = forecast_data.id
      left join cybersource_data
      on trafic_data.id = cybersource_data.id
   order by date desc    
)

select * from global_data
where country not in ('UK','RU')
order by date desc 




