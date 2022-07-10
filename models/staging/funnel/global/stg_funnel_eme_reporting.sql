{{
    config(
        materialized='table',
        labels={'type': 'funnel_data', 'contains_pie': 'no', 'category': 'staging'},
    )
}}

with
    funnel_data as (

       select * from {{ ref('stg_funnel_country_perf') }}
    )

select
    date,
    country,
    channel_grouping,
    sum(cost) as cost,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(revenue) as revenue
from funnel_data
where campaign_type = 'PERF'
group by 1, 2, 3







