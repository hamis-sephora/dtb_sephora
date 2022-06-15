


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
 Ad_account_name__Snapchat,
 Campaign_Name__Snapchat,
 Squad_Name__Snapchat,
 Ad_Type__Snapchat ,
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
 sum(Impressions__Snapchat) as impresssions_snapchat, 
 sum(Video_Views__Snapchat) as video_view_snapchat, 
 sum(Spend__Snapchat) as cost_snapchat
from
 {{ref('stg_funnel_global_data')}} 
  where Data_Source_type = 'snapchat'
group by 1, 2,3,4,5,6,7,8,9,10
