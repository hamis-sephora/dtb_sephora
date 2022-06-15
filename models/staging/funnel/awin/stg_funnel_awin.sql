


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
    advertiser_country__awin,
    advertiser_name__awin,
    advertiser_id__awin,
    publisher_name__awin,
    publisher_id__awin,
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
    sum(impressions__awin) as impressions_awin,
    sum(clicks__awin) as clicks_awin,
    sum(total_amount__awin) as amount_awin,
    sum(total_commission__awin) as commission_awin,
    sum(transaction_commission__awin) as transaction_comission_awin,
    sum(transaction_amount__awin) as transaction_amount_awin
from {{ ref('stg_funnel_global_data') }}
where data_source_type = 'awin'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
