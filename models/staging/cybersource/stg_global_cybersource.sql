
{{
    config(
        materialized='table',
        labels={'type': 'cybersource', 'contains_pie': 'no', 'category': 'staging'},
    )
}}

with
    cybersource_consolidation as (

        select
            parse_date('%Y%m%d', date) as date,
            country,
            case
                when country in ('ru Sephora', 'ru IDB')
                then 'ru'
                when country in ('bh', 'qa', 'ae', 'sa', 'kw', 'om')
                then 'me'
                 when country in ('DK','SE') then 'SCANDI' 
                else country
            end as country_bis,
            datasource_cs,
            (sessions),
            case when (datasource_cs = 'app') then sessions else 0 end as sessions_app,
            case
                when (datasource_cs != 'app') then sessions else 0
            end as sessions_website,
            case
                when (datasource_cs = 'app') then transactions else 0
            end as transactions_app,
            case
                when (datasource_cs = 'app') then revenue_local else 0
            end as revenue_local_app,
            case
                when (datasource_cs != 'app') then transactions else 0
            end as transactions_website,
            transactions,
            revenue_local,
            case
                when country in ('pl', 'PL')
                then revenue_local / 4.5
                when country in ('cz', 'CZ')
                then revenue_local / 26.75
                when country in ('ae', 'AE')
                then revenue_local / 4.187
                when country in ('sa', 'SA')
                then revenue_local / 4.275
                when country in ('dk', 'DK')
                then revenue_local / 7.6
                when country in ('se', 'SE')
                then revenue_local / 11
                when country in ('om', 'OM')
                then revenue_local / 0.46
                when country in ('bh', 'BH')
                then revenue_local / 0.46
                when country in ('kw', 'KW')
                then revenue_local / 0.36
                when country in ('ru', 'RU', 'ru Sephora', 'ru IDB')
                then revenue_local / 88.69
                when country in ('qa', 'QA')
                then revenue_local / 4.385
                when country in ('ro', 'RO')
                then revenue_local / 4.895
                else revenue_local
            end as revenue_euro
        from {{ source('cy_data', 'cbs_ga_fb_aggregated') }}
        where revenue_local is not null and country not in ('DE Zalando', 'ERROR')
    )
select
    date,
    upper(country_bis) as country,
    datasource_cs,
    sessions,
    sessions_app,
    sessions_website,
    transactions,
    transactions_app,
    transactions_website,
    revenue_local,
    revenue_euro
from cybersource_consolidation
