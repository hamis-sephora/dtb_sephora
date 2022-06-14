{{
  config(
    materialized = 'table',
    labels = {'type': 'cybersource', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

with forecast as (
select
  parse_date('%Y%m%d',date) as Date,
  country,
  sum(cast(_turnoverforecast_ as float64)) as _turnoverforecast_,
  sum(cast(sessions_forecast as float64)) as sessions_forecast,
  sum(cast(Orders_forecast as float64)) as Orders_forecast,
  round(sum(cast(Budget AS float64)),2) AS budget, 

  --sum(cast(Monthly_Budget as float64)) as Monthly_Budget
  from  {{ source('forecast', 'forecast_data') }} 
group by 1 , 2
) 
select Date , 
       case when country in ('UAE', 'QA', 'KW' , 'BH' , 'KSA' ,'OM' ) then 'ME'
            when country in ( 'IDB' , 'RU') then 'RU'
            when country in ('SE','DK') then 'SCANDI'
       else country end as country, 
       sessions_forecast , 
       _turnoverforecast_, 
       Orders_forecast,
       budget
       from forecast
       order by date desc 

       