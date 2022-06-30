{{
    config(
        materialized='table',
        labels={
            'type': 'funnel_google_analytics_cbs',
            'contains_pie': 'no',
            'category': 'reporting',
        },
    )
}}

with
    ga_data as (
        select
            date,
            cast(
                concat(date, '_', country, '_', channel, '_', platform) as string
            ) as id_channel,
            week,
            country,
            channel,
            platform,
            sessions,
            addtocart,
            sessions_retrieved,
            transactions,
            revenue_local,
            revenue_euro
        from {{ ref('prd_all_trafic') }}
    ),

    media_data as (
        select
            date,
            country,
            channel_grouping as channel,
            'website' as platform,
            cost,
            impressions,
            clicks
        from {{ ref('stg_funnel_eme_reporting') }}
    ),

    consolidation_media as (
        select
            date,
            cast(
                concat(
                    media_data.date,
                    '_',
                    media_data.country,
                    '_',
                    media_data.channel,
                    '_',
                    media_data.platform
                ) as string
            ) as id_channel,
            country,
            platform,
            channel,
            impressions,
            clicks,
            cost
        from media_data
    ),
global_data as (
select
    ga_data.date,
    ga_data.id_channel,
    ga_data.week,
    ga_data.country,
    ga_data.channel,
    case
        when
            ga_data.channel in (
                'Direct', 'Organic Search', 'Referral', '(Other)', 'app', 'organic'
            )
        then 'ORGANIC'
        when ga_data.channel in ('Emailing', 'Eprm')
        then 'OWN MEDIA'
        else 'PAID MEDIA'
    end as channel_grouping,
    ga_data.platform,
    ga_data.sessions,
    ga_data.addtocart,
    ga_data.sessions_retrieved,
    ga_data.transactions,
    ga_data.revenue_local,
    ga_data.revenue_euro,
    consolidation_media.id_channel as media_id_channel,
    consolidation_media.country as media_country,
    consolidation_media.platform as media_platform,
    consolidation_media.channel as media_channel,
    consolidation_media.impressions as media_impressions,
    consolidation_media.clicks as media_clicks,
    consolidation_media.cost as media_cost
from ga_data
left join consolidation_media on ga_data.id_channel = consolidation_media.id_channel
)

select * from global_data
where country not in ('RU', 'UK')
order by date desc 




