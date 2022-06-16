

{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}


select
 Date, 
 Country_ as country, 
 Data_Source_type,
 Data_Source_name,
 Campaign_Name__Facebook_Ads,
 Ad_Set_Name__Facebook_Ads, 
 Ad_Name__Facebook_Ads,
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
 sum(Impressions__Facebook_Ads) as impresssions_facebook, 
 sum(Clicks_all__Facebook_Ads) as clicks_facebook, 
 sum(Amount_Spent__Facebook_Ads) as cost_facebook
from
  {{ref('stg_funnel_global_data')}} 
  where Data_Source_type = 'facebookads'
group by 1, 2,3,4,5,6,7,8,9
