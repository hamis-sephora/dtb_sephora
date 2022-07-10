{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}
select  
      Date , 
      Country_ as country, 
      Data_Source_type as regie_source, 
            campaign,
      media_type, 
case
                when
                    Data_Source_type in (
                        'facebookads', 'snapchat'
                    ) and lower(campaign) like '%_ec_%' and lower(campaign) not like '%ecom%'
                then 'PERF'
                when Data_Source_type = 'facebook' and campaign like '%_CS_TRAF_%' and lower(campaign) not like '%coad%'
                then 'PERF'
                when
                    Data_Source_type = 'adwords'
                    and media_type = 'Display'
                    and campaign not like '%_EC_%'
                then 'OTHERS'
                when
                    Data_Source_type = 'adwords'
                    /*--and media_type = 'Display'*/
                    and lower(campaign) like '%_ec_%'
                then 'PERF'
                when Data_Source_type in ('criteo') and lower(campaign) not like '%trademarketing%' then 'PERF'  
                when Data_Source_type in ('rtbhouse', 'awin', 'tiktok')                
                then 'PERF'
                else 'OTHERS'
             end as campaign_type,
        
            case
                when Data_Source_type in ('facebookads', 'tiktok', 'snapchat')
                then 'Social'
                when Data_Source_type in ('rtbhouse', 'criteo')
                then 'Retargeting'
                when Data_Source_type ='SCANDI' and lower(campaign) like '%_bra_%' then 'Paid Search Brand'
                when Data_Source_type in ('awin')
                then 'Affiliation'
                when
                    Data_Source_type = 'adwords'
                    and lower(campaign) like '%brandsephora%'
                    or lower(campaign) like '%brand sephora%'
                    or lower(campaign) like '%brandbidding%'
                    or lower(campaign) like '%core-brand%'
                    or lower(campaign) like '%bra-exact%'
                    or lower(campaign) like '%sephora brand%'
                    or lower(campaign) like '%exclusivebrand%'
                then 'Paid Search Brand'
                /*
                when
                    regie_source = 'adwords'
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
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost
from {{ref('stg_funnel_global_data')}} 
group by 1,2,3,4,5,6