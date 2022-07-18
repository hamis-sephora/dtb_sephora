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

        select
            Date,
            country_grouping as country,
            case when platform in ('iOS', 'Android') then 'app' else 'website' end as platform,
            concat(ga_source,'/',ga_medium) as channel, 
            channel_grouping,
            sum(sessions) as sessions,
            sum(addtocart) as addtocart,
            sum(transactions) as transactions,
            sum(revenue_euro) as revenue_euro
        from {{ ref('stg_ga4_performance') }}
        group by 1, 2, 3, 4, 5
        having platform = 'app'



