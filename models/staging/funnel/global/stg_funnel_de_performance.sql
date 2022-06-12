{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}
select  
      Date , 
      country, 
      Data_Source_type as regie_source, 
            campaign,
      media_type, 
            case    
                when
                    data_source_type in ('facebookads', 'snapchat'
                    ) and campaign like '%_EC_%'
                      and campaign not like '%ECOM%'
                then 'PERF'
               when data_source_type ='adwords' and media_type='Display' 
                and campaign not like '%_EC_%' then 'OTHERS'                
               when data_source_type ='adwords' or media_type='Display' 
                and campaign not like '%_EC_%' then 'PERF'
                when data_source_type in ('rtbhouse', 'criteo', 'awin', 'tiktok')
                then 'PERF'
                else 'OTHERS'
            end as campaign_type,
            case
                when data_source_type in ('facebookads', 'tiktok', 'snapchat')
                then 'Social'
                when data_source_type in ('rtbhouse', 'criteo')
                then 'Retargeting'
                when data_source_type in ('awin')
                then 'Affiliation'
                when
                    data_source_type = 'adwords'
                    and campaign like '%BrandSephora%'
                    or campaign like '%Brand Sephora%'
                    or campaign like '%BrandBidding%'
                    or campaign like '%Core-Brand%'
                    or campaign like '%BRA-Exact%'
                    or campaign like 'sephora brand'
                then 'Paid Search Brand'
                /*
                when
                    data_source_type = 'adwords'
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