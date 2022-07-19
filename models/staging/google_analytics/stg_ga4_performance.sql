{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

select
    Date , 
    country, 
    case when country in ('KSA', 'UAE') then 'ME' else country end as country_grouping,
    platform,
    ga_campaign,
    ga_medium,
    ga_source,
    case
        when
            lower(ga_medium) like '%socialmedia%' or lower(
                ga_medium
            ) like '%social%' or lower(ga_medium) in (
                'fb',
                'tw',
                'ig',
                'yt',
                'facebook',
                'instagram',
                'twitter',
                'snapchat',
                'pinterest',
                'igshopping',
                'tiktok'
            ) or lower(ga_source) in (
                'fb',
                'tw',
                'ig',   
                'yt',
                'facebook',
                'instagram',
                'twitter',
                'snapchat',
                'pinterest',
                'igshopping',
                'tiktok'
            ) or lower(ga_medium) like '%influencers%' or lower(ga_medium) like '%instagram%' or lower(ga_medium) like '%facebook%'
        then 'Social'
        when lower(ga_source) = 'games' or lower(ga_medium) = 'games'
        then 'Games'
        when
            lower(ga_medium) in ('rtg', 'retargeting') or lower(ga_source) in (
                'crt', 'criteo', 'rtghouse', 'rtb'
            ) or lower(ga_campaign) like '%GDN%'
        then 'Retargeting'
        when lower(ga_medium) like '%partner%'
        then 'Partnership'
        when
            lower(ga_medium) like '%email%' or lower(
                ga_medium
            ) like '%splio%' or ga_medium = 'sms' or lower(ga_medium) like '%push-web%' or lower(ga_medium) like '%ecrm%' or lower(ga_medium) like '%crm%' or lower(ga_medium) like '%in-app%' or lower(ga_medium) like '%push-app%'
        then 'Emailing'
        when
            lower(ga_medium) = 'affiliate' or lower(
                ga_source
            ) like '%awin%' or lower(ga_source) like '%cj' or lower(
                ga_source
            ) like '%affiliation%' or lower(ga_source) like '%idealo%' or lower(ga_medium) like '%affiliation%'
        then 'Affiliation'
        when lower(ga_medium) like '%cpc%' and lower(ga_campaign) like '%brand%' or lower(ga_medium) like '%cpc%' and lower(ga_campaign) like '%brandsephora%'
        then 'Paid Search Brand'
        when lower(ga_medium) like '%cpc' and lower(ga_campaign) not like '%brand%'or lower(ga_medium) like '%cpc%' and lower(ga_source) like '%google%'
        then 'Paid Search Non Brand'
        when lower(ga_medium) like '%display%' or
         lower(ga_source) like '%display%' or 
         lower(ga_medium) like '%cpm%' or 
         lower(ga_medium) like '%banner%' or 
         lower(ga_medium) like '%youtube%' or 
         lower(ga_medium) like '%video%' or 
         lower(ga_source) like '%youtube%' or
         lower(ga_medium) like '%media%' 
         then 'Media' 
        when lower(ga_medium) like '%organic%' then 'Organic Search'
        when lower(ga_medium) like '%referral%' then 'Referral'
        when lower(ga_source) like '%apple%' then 'Search Apple'
        when lower(ga_source) = '(direct)' then 'Direct'
        when lower(ga_medium) = '%search%' and lower(ga_campaign) like '%appdownload%' then 'App Download'
        when lower(ga_medium) like '%in-app%' or lower(ga_medium) like '%push-app%' then 'Emailing'
        else '(Other)'
    end as channel_grouping,
    sum(sessions) as sessions,
    sum(add_to_cart) as addtocart,
    sum(transactions) as transactions, 
    sum(revenue_euro) as revenue_euro, 
    sum(total_revenue) as total_revenue,         
from {{ ref('stg_ga4_raw_data') }}
where platform != 'web'
group by 1, 2, 3, 4, 5, 6, 7
order by date asc















