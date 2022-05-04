

{{
    config(
        materialized='table',
        labels={'type': 'funnel_data', 'contains_pie': 'no', 'category': 'staging'},
    )
}}


select
    date,
    country,
    data_source_type,
    data_source_name,
    category_name__criteo,
    advertiser__criteo,
    ad_set__criteo,
    case
        when
            data_source_type in (
                'adwords', 'facebookads', 'snapchat', 'tiktok'
            ) and campaign like '%_EC_%'
        then 'PERF'
        when data_source_type in ('rtbhouse', 'criteo', 'awin')
        then 'PERF'
        else 'OTHERS'
    end as campaign_type,
    case
        when
            data_source_type in (
                'adwords', 'facebookads', 'snapchat', 'tiktok'
            ) and campaign like '%SephoraEUR_SR%'
        then 'OK'
        else 'NOT OK'
    end as nomenclature,
    sum(cost__criteo) as cost_criteo,
    sum(impressions__criteo) as impression_criteo,
    sum(order_value__criteo) as order_value_criteo,
    sum(clicks__criteo) as clicks_criteo
from {{ ref('stg_funnel_global_data') }}
where data_source_type = 'criteo'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
