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
                when
                    regie_source in (
                        'facebookads', 'snapchat'
                    ) and campaign like '%_EC_%' and campaign not like '%ECOM%'
                then 'PERF'
                when regie_source = 'facebook' and campaign like '%_CS_TRAF_%' and campaign not like '%coad%'
                then 'PERF'
                when
                    regie_source = 'adwords'
                    and media_type = 'Display'
                    and campaign not like '%_EC_%'
                then 'OTHERS'
                when
                    regie_source = 'adwords'
                    or media_type = 'Display'
                    and campaign not like '%_EC_%'
                then 'PERF'
                when regie_source in ('rtbhouse', 'criteo', 'awin', 'tiktok')
                then 'PERF'
                else 'OTHERS'
            end as campaign_type,
            case
                when regie_source in ('facebookads', 'tiktok', 'snapchat')
                then 'Social'
                when regie_source in ('rtbhouse', 'criteo')
                then 'Retargeting'
                when regie_source in ('awin')
                then 'Affiliation'
                when
                    regie_source = 'adwords'
                    and campaign like '%BrandSephora%'
                    or campaign like '%Brand Sephora%'
                    or campaign like '%BrandBidding%'
                    or campaign like '%Core-Brand%'
                    or campaign like '%BRA-Exact%'
                    or campaign like 'sephora brand'
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















