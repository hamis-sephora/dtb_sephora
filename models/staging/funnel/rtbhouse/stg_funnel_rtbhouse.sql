
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
 Sub_Campaign__RTB_House,
 Device_type__RTB_House,
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
 sum(Impressions__RTB_House) as impressions_rtbhouse, 
 sum(Clicks__RTB_House	) as clicks_rtbhouse, 
 sum(Campaign_Cost__RTB_House) as cost_rtbhouse
from
  {{ref('stg_funnel_global_data')}} 
  where Data_Source_type = 'rtbhouse'
group by 1, 2,3,4,5,6,7,8