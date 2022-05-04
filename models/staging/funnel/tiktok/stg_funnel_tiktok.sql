

{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

select
 Date, 
 country, 
 Data_Source_type,
 Data_Source_name,
 Advertiser_name__TikTok,
 Campaign_name__TikTok,
 Adgroup_name__TikTok,
 Ad_name__TikTok ,
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
 sum(Impressions__TikTok) as impresssions_tiktok, 
 sum(Clicks__TikTok) as clicks_tiktok, 
 sum(Total_cost__TikTok) as cost_tiktok
from
  {{ref('stg_funnel_global_data')}} 
  where Data_Source_type = 'tiktok'
group by 1, 2,3,4,5,6,7,8,9,10
