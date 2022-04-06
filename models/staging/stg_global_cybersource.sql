
{{ config(materialized='table') }}


  with revenue as (

  SELECT
    PARSE_DATE('%Y%m%d',Date) as date, 
    country, 
    case when country in ('ru Sephora', 'ru IDB') then 'ru' 
     when country in ('bh','qa','ae','sa','kw','om') then 'me' else country end as country_bis, 
    datasource_cs,
    (sessions),
    CASE WHEN (datasource_cs = 'app') THEN sessions ELSE 0 END AS sessions_app,
    CASE WHEN (datasource_cs != 'app') THEN sessions ELSE 0 END AS sessions_website,
    CASE WHEN (datasource_cs = 'app') THEN transactions ELSE 0 END AS transactions_app,
    CASE WHEN (datasource_cs = 'app') THEN revenue_local ELSE 0 END AS revenue_local_app,
    CASE WHEN (datasource_cs != 'app') THEN transactions ELSE 0 END
    AS transactions_website,
    transactions,
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
        else revenue_local end as revenue_euro
  FROM
   {{ source('cy_data', 'cbs_ga_fb_aggregated') }} 
   where  revenue_local is not null and country not in ('DE Zalando','ERROR')
  )
  , 

 consolidation as (

  select 
         extract (week(sunday) from date) as week_number , 
         extract( year from date ) as year,
         country_bis as country, 
         sum(transactions) as revenue, 
         round(sum(revenue_local),2) as revenue_local,
         round(sum(revenue_euro),2) as revenue_euro
         from revenue
         group by 1,2,3

 )

 select 
       week_number,
       upper(country) as country, 
       sum(case when year=2022 then revenue_euro else 0 end) as revevue_2022,
       sum(case when year=2021 then revenue_euro else 0 end ) as revevue_2021,
       from consolidation
       where week_number > 0
       group by 1 ,2
       order by 1 asc 
