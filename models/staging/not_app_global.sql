{{ config(materialized='table') }}


Select Date, case when country in ('ru Sephora', 'ru IDB') then 'ru' else country end as country, 'Desktop' as channel, sum(sessions) as sessions 

FROM {{ source('cy_data', 'cbs_ga_fb_aggregated') }} 

where datasource_cs != 'app'
and country in ('ru Sephora', 'ru IDB')
and sessions is not null
and date >= cast(Date_sub(DATE_TRUNC(current_date, Month), interval 1 MONTH) as string) 
group by 1,2