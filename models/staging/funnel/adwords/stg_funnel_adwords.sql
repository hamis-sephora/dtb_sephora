
{{
    config(
        materialized='table',
        labels={'type': 'funnel_data', 'contains_pie': 'no', 'category': 'staging'},
    )
}}

select
    date,
    Country_ as country, 
    data_source_type,
    data_source_name,
    campaign__adwords,
    campaign_type__adwords,
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
    sum(impressions__adwords) as impressions_adwords,
    sum(clicks__adwords) as clicks_adwords,
    sum(interactions__adwords) as interaction_adwords,
    sum(cost__adwords) as cost_adwords

from {{ ref('stg_funnel_global_data') }}
where data_source_type = 'adwords'
group by 1, 2, 3, 4, 5, 6, 7,8
