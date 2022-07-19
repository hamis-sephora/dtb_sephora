{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

with data_media as (
select   
     distinct 
      Date , 
      Country_ as country, 
      regie_source, 
      source_name, 
      campaign,
      media_type, 
            case
                when lower(campaign)  like '%coads%' then 'OTHERS'
                when regie_source in ('facebookads', 'snapchat') and lower(campaign) like '%_ec_%' and lower(campaign) not like '%ecom%'
                then 'PERF'
                when regie_source = 'facebook' and campaign like '%_CS_TRAF_%' 
                then 'PERF'
                when regie_source = 'tiktok' and lower(campaign) not like '%coad%'
                then 'PERF'                
                when regie_source = 'adwords' and media_type = 'Display' and campaign not like '%_EC_%'
                then 'OTHERS'
                when regie_source='adwords' and lower(campaign) like '%_perf%' then 'PERF'
                when regie_source = 'adwords' /*--and media_type = 'Display'*/ and lower(campaign) like '%_ec_%'
                then 'PERF'
                when regie_source in ('criteo') and lower(campaign) not like '%trademarketing%' then 'PERF'  
                when regie_source in ('rtbhouse', 'awin')
                then 'PERF'
                else 'OTHERS'
             end as campaign_type,
        
            case
                when regie_source in ('facebookads', 'tiktok', 'snapchat')
                then 'Social'
                when regie_source in ('rtbhouse', 'criteo')
                then 'Retargeting'
                when Country_ ='SCANDI' and lower(campaign) like '%_bra_%' then 'Paid Search Brand'
                when regie_source in ('awin')
                then 'Affiliation'
                when
                    regie_source = 'adwords'
                    and lower(campaign) like '%brandsephora%'
                    or lower(campaign) like '%brand sephora%'
                    or lower(campaign) like '%brandbidding%'
                    or lower(campaign) like '%core-brand%'
                    or lower(campaign) like '%bra-exact%'
                    --or lower(campaign) like '%sephora brand%'
                    --or lower(campaign) like '%exclusivebrand%'
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
      sum(cost) as cost,
      sum(revenue) as revenue
from {{ref('stg_funnel_global_report')}} 
group by 1,2,3,4,5,6,7,8
)

select 
      * 
from data_media
where country not in ('NOT NOW', 'UNKNOW', 'UK' , 'RU' ) or country is null 















