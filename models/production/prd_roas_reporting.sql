{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'reporting'}  
   )
}}

with prd_session as (
select
    Date as date  , 
    country, 
    platform, 
    sessions, 
    addtocart, 
    searches, 
    list_scroll, 
    skincare, 
    fragrance, 
    makeup, 
    hair,   
    conversions, 
    revenue_local, 
    revenue_euros, 
    sessions_retrieved, 
    sessions_forecast ,
    budget,
    sessions_cbs,
    conversions_cbs,  
    revenue_euros_cbs,
    revenue_local_cbs,
from   
  {{ref('prd_session_d')}} 
order by Date desc , country asc 
) , 

report_media as (
select  
      Date , 
      country, 
      'website' as platform,
      cost_adwords, 
      cost_awin, 
      cost_criteo, 
      cost_facebookads, 
      cost_rtbhouse, 
      cost_snapchat, 
      cost_tiktok,
      cost
from {{ref('int_report_media_cost')}} 
)

select 
    prd_session.Date as date  , 
    prd_session.country, 
    prd_session.platform, 
    sessions, 
    addtocart, 
    searches, 
    list_scroll, 
    skincare, 
    fragrance, 
    makeup, 
    hair,   
    conversions, 
    revenue_local, 
    revenue_euros, 
    sessions_retrieved, 
    sessions_forecast ,
    budget,
    sessions_cbs,
    conversions_cbs,  
    revenue_euros_cbs,
    revenue_local_cbs , 
    cost_adwords, 
    cost_awin,
    cost_criteo,
    cost_facebookads,
    cost_rtbhouse, 
    cost_snapchat,
    cost_tiktok,
    cost
from prd_session
left join report_media
 on concat(prd_session.date,'_',prd_session.platform,'_',prd_session.country) = concat(report_media.date,'_',report_media.platform,'_',report_media.country)












