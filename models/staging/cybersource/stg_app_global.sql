{{
  config(
    materialized = 'table',
    labels = {'type': 'cybersource', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

-- Trafic App tout pays ( source : cybersource )
with date_range as (
select
    '20190101' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date 
    ) ,
  
data as (
Select  PARSE_DATE('%Y%m%d',Date) as date, 
case when country in ('ru Sephora', 'ru IDB') then 'ru' 
     when country in ('bh','qa','ae','sa','kw','om') then 'me' else country end as country, 
     'App' as channel, 
     'App' as platform,
     'App' as device, 
     sum(sessions) as sessions ,
     sum(transactions) as conversions, 
     round(sum(revenue_local),2) as revenue_local, 
     round (sum(revenue_local) / sum(transactions),2 ) as aov
FROM {{ source('cy_data', 'cbs_ga_fb_aggregated') }} ,
date_range
where datasource_cs = 'app'
and sessions is not null
and date between start_date and end_date
group by 1,2
order by date desc )
select 
       Date, 
       country, 
       channel, 
       platform,
       device, 
       sessions, 
       conversions ,
       revenue_local,
       case when country in ('pl', 'PL') then revenue_local/4.5 
        when country in ('cz', 'CZ') then revenue_local/26.75
        when country in ('ae', 'AE') then revenue_local/4.187
        when country in ('sa', 'SA') then revenue_local/4.275
        when country in ('dk','DK') then revenue_local/7.6
        when country in ('se', 'SE') then revenue_local/11
        when country in ('om', 'OM') then revenue_local/0.46
        when country in ('bh', 'BH') then revenue_local/0.46
        when country in ('kw', 'KW') then revenue_local/0.36
        when country in ('ru', 'RU', 'ru Sephora', 'ru IDB') then revenue_local/88.69
        when country in ('qa', 'QA') then revenue_local/4.385
        when country in ('ro', 'RO') then revenue_local/4.895
       else revenue_local end as revenue
       from data 