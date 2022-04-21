{{ config(materialized='table') }}


with data as (

select 
    Date as date, 
    extract(week(sunday) from date) as week, 
    country, 
    channel , 
    platform,
    sessions, 
    sessions_rattrapee, 
    transactions, 
    revenue_local , 
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
    from  {{ref('stg_sessions_retrieved_channel')}} 


 UNION ALL 

 select 
     Date as date,
    extract(week(sunday) from date) as week, 
     UPPER(country) as country, 
     channel, 
     platform,
     sessions, 
     0 as sessions_rattrapee , 
     conversions as transactions,
     revenue_local, 
     revenue 
     from {{ref('stg_app_global')}} 


UNION ALL 
  select
     Date as date,
    extract(week(sunday) from date) as week, 
     UPPER(country) as country, 
     channel, 
     platform,
     sessions, 
     0 as sessions_rattrapee , 
     conversions as transactions,
     revenue_local, 
     revenue 
     from {{ref('stg_not_app_global')}}           
)

select
      date, 
      extract(week(sunday) from date) as week, 
      country, 
      channel, 
      case when channel in ('Direct', 'Organic Search','Referral', '(Other)', 'App') then 'ORGANIC'
           when channel in ('Emailing','Eprm') then 'OWN MEDIA'
           else 'PAID MEDIA'
           end as channel_grouping, 
      platform, 
      sessions, 
      sessions_rattrapee,
      case when platform = 'website' and country not in ('RU','ME') and date > '2021-10-01' then sessions_rattrapee
      else sessions end as sessions_retrieved,
      transactions,
      revenue_local,
      revenue ,
      0 as session_addtocart
      from data

UNION ALL 

select 
       Date, 
       extract(week(sunday) from date) as week, 
       country, 
       channel , 
       channel_grouping,  
       platform,
       0 as sessions , 
       0 as sessions_rattrapee,
       0 as sessions_retrieved,
       0 as transactions,
       0 as revenue_local,
       0 as revenue  , 
       session_addtocart as session_addtocart,       
       from {{ref('stg_ga_addtocart')}}     







