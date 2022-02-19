{{ config(materialized='table') }}

select date,
case when country in ('UAE', 'KSA', 'OM', 'BH', 'KW', 'QA') then 'ME' 
        when country in ('RU','IDB') then 'RU' else country end as country, 
sum(sessions_forecast) as Sessions_objective  
FROM {{ source('forecast', 'Forecast_turnover_sessions') }} 
group by 1,2