{{
    config(
        materialized='table',
        labels={'type': 'funnel_data', 'contains_pie': 'no', 'category': 'staging'},
    )
}}

with
    funnel_data as (

        select
            date,
            country,
            data_source_type,
            campaign,
            media_type,
            case
                when
                    data_source_type in (
                        'facebookads', 'snapchat'
                    ) and campaign like '%_EC_%' and campaign not like '%ECOM%'
                then 'PERF'
                when data_source_type = 'facebook' and campaign like '%_CS_TRAF_%' and campaign not like '%coad%'
                then 'PERF'
                when
                    data_source_type = 'adwords'
                    and media_type = 'Display'
                    and campaign not like '%_EC_%'
                then 'OTHERS'
                when
                    data_source_type = 'adwords'
                    or media_type = 'Display'
                    and campaign not like '%_EC_%'
                then 'PERF'
                when data_source_type in ('rtbhouse', 'criteo', 'awin', 'tiktok')
                then 'PERF'
                else 'OTHERS'
            end as campaign_type,
            case
                when data_source_type in ('facebookads', 'tiktok', 'snapchat')
                then 'Social'
                when data_source_type in ('rtbhouse', 'criteo')
                then 'Retargeting'
                when data_source_type in ('awin')
                then 'Affiliation'
                when
                    data_source_type = 'adwords'
                    and campaign like '%BrandSephora%'
                    or campaign like '%Brand Sephora%'
                    or campaign like '%BrandBidding%'
                    or campaign like '%Core-Brand%'
                    or campaign like '%BRA-Exact%'
                    or campaign like 'sephora brand'
                then 'Paid Search Brand'
                /*
                when
                    data_source_type = 'adwords'
                    and campaign not like '%BrandSephora%'
                    or campaign not like '%Brand Sephora%'
                    or campaign not like '%BrandBidding%'
                    or campaign not like '%Core-Brand%'
                    or campaign not like '%BRA-Exact%'
                    or campaign not like 'sephora brand'
                then 'Paid Search Non Brand'
                */
                else 'Paid Search Non Brand'
            end as channel_grouping,
            cost,
            impressions,
            clicks
        from {{ ref('stg_funnel_global_data') }}
    )

select
    date,
    country,
    channel_grouping,
    sum(cost) as cost,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from funnel_data
where campaign_type = 'PERF'
group by 1, 2, 3







