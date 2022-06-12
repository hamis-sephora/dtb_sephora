{{
    config(
        materialized='table',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'no',
            'category': 'staging',
        },
    )
}}

with
    data_info as (

        select
            parse_date('%Y%m%d', event_date) as date,
            case when country in ('KSA', 'UAE') then 'ME' else country end as country,
            case when platform in ('IOS', 'ANDROID') then 'app' else 'website' end as platform,
            channel_grouping as channel,
            channel_grouping,
            sum(sessions) as sessions,
            sum(addtocart) as addtocart,
            sum(add_to_cart) as add_to_cart,
            sum(transactions) as transactions,
            sum(revenue) as revenue_local
        from {{ ref('stg_ga4_performance') }}
        group by 1, 2, 3, 4, 5
        having platform = 'app'
    )

select 
date, 
country, 
platform,
channel,
channel_grouping,
sessions,
addtocart,
add_to_cart,
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
from data_info



