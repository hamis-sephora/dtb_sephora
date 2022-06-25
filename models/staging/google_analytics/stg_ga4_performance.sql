{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

select
    distinct 
    event_date,
    country,
    platform,
    name,
    medium,
    source,
    case
        when
            lower(medium) like '%socialmedia%' or lower(
                medium
            ) like '%social%' or lower(medium) in (
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
            ) or lower(source) in (
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
            ) or lower(medium) like '%influencers%' or lower(medium) like '%instagram%' or lower(medium) like '%facebook%'
        then 'Social'
        when lower(source) = 'games' or lower(medium) = 'games'
        then 'Games'
        when
            lower(medium) in ('rtg', 'retargeting') or lower(source) in (
                'crt', 'criteo', 'rtghouse', 'rtb'
            ) or lower(name) like '%GDN%'
        then 'Retargeting'
        when lower(medium) like '%partner%'
        then 'Partnership'
        when
            lower(medium) like '%email%' or lower(
                medium
            ) like '%splio%' or medium = 'sms' or lower(medium) like '%push-web%' or lower(medium) like '%ecrm%' or lower(medium) like '%crm%' or lower(medium) like '%in-app%' or lower(medium) like '%push-app%'
        then 'Emailing'
        when
            lower(medium) = 'affiliate' or lower(
                source
            ) like '%awin%' or lower(source) like '%cj' or lower(
                source
            ) like '%affiliation%' or lower(source) like '%idealo%' or lower(medium) like '%affiliation%'
        then 'Affiliation'
        when lower(medium) like '%cpc%' and lower(name) like '%brand%' or lower(medium) like '%cpc%' and lower(name) like '%brandsephora%'
        then 'Paid Search Brand'
        when lower(medium) like '%cpc' and lower(name) not like '%brand%'or lower(medium) like '%cpc%' and lower(source) like '%google%'
        then 'Paid Search Non Brand'
        when lower(medium) like '%display%' or
         lower(source) like '%display%' or 
         lower(medium) like '%cpm%' or 
         lower(medium) like '%banner%' or 
         lower(medium) like '%youtube%' or 
         lower(medium) like '%video%' or 
         lower(source) like '%youtube%' or
         lower(medium) like '%media%' 
         then 'Media' 
        when lower(medium) like '%organic%' then 'Organic Search'
        when lower(medium) like '%referral%' then 'Referral'
        when lower(source) like '%apple%' then 'Search Apple'
        when lower(source) = '(direct)' then 'Direct'
        when lower(medium) = '%search%' and lower(name) like '%appdownload%' then 'App Download'
        when lower(medium) like '%in-app%' or lower(medium) like '%push-app%' then 'Emailing'
        else '(Other)'
    end as channel_grouping,
    sum(sessions) as sessions,
    sum(addtocart) as addtocart,
    sum(add_to_cart) as add_to_cart,
    sum(users) as users,
    sum(transactions) as transactions, 
    sum(revenue) as revenue    
from {{ source('ga4', 'ga4_data') }} 
where platform != 'WEB'
group by 1, 2, 3, 4, 5, 6















