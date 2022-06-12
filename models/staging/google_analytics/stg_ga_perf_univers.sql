{{
    config(
        materialized='incremental',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'no',
            'category': 'staging',
        },
    )
}}
with
    date_range as (
        select
    format_date('%Y%m%d',date_sub(current_date(), interval 893 day)) as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date 
    ),
    data as (
        select
            parse_date('%Y%m%d', date) as date,
            extract(year from parse_date('%Y%m%d', date)) as year,
            case
                when
                    regexp_contains(
                        h.page.hostname,
                        r'^(www|m|api)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk)$'
                    )
                then lower(substr(h.sourcepropertyinfo.sourcepropertydisplayname, 9, 2))
                when regexp_contains(h.page.hostname, r'^(www)\.sephora\.(ae)$')
                then 'me'
                when regexp_contains(h.page.hostname, r'^(www)\.sephora\.(sa)$')
                then 'me'
                when regexp_contains(h.page.hostname, r'^(www)\.sephora\.(om)$')
                then 'me'
                when regexp_contains(h.page.hostname, r'^(www)\.sephora\.(bh)$')
                then 'me'
                when regexp_contains(h.page.hostname, r'^(www)\.sephora\.(com.kw)$')
                then 'me'
                when regexp_contains(h.page.hostname, r'^(www)\.sephora\.(qa)$')
                then 'me'
                WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www)\.sephora\.(com.tr)$') THEN 'tr'                   
                when regexp_contains(h.page.hostname, r'^(www)\.sephora\.(ro)$')
                then 'ro'
                when regexp_contains(h.page.hostname, r'^(www)\.sephora\.(gr)$')
                then 'gr'
                else 'empty'
            end as country,
            channelgrouping as channel,
            count(
                distinct case
                    when h.ecommerceaction.action_type = '3'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as addtocart,
            count(
                distinct case
                    when h.eventinfo.eventcategory = 'search'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as searches,
            count(
                distinct case
                    when
                        h.eventinfo.eventcategory = 'product list'
                        and h.eventinfo.eventaction = 'list scroll'
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as list_scroll,
            count(
                distinct case
                    when h.contentgroup.contentgroup1 in('skincare','pielęgnacja twarzy','العناية بالبشرة')
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as skincare,
            count(
                distinct case
                    when h.contentgroup.contentgroup1 in ('fragrance','parf','perfumy')
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as fragrance,
            count(
                distinct case
                    when h.contentgroup.contentgroup1 in('makeup','make up','cat_Maquillage','cat maquillage','makijaż','المكياج')
                    
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as makeup,
            count(
                distinct case
                    when h.contentgroup.contentgroup1 in('hair','العناية بالشعر','włosy')
                    then concat(fullvisitorid, cast(visitstarttime as string))
                end
            ) as hair,
            count(
                distinct concat(fullvisitorid, cast(visitstarttime as string))
            ) as sessions,
        -- COUNT(DISTINCT h.transaction.transactionId) AS transactions,
        -- round(sum( h.transaction.transactionRevenue/1000000),2) as revenue_local ,
        from {{ source('ga_data', 'ga_sessions_*') }}, unnest(hits) as h, date_range
        where
        REGEXP_CONTAINS(h.page.hostname, r'^(www|m)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk|sa|ae|com.kw|om|bh|qa|ro|gr|com.tr)$') IS TRUE 
            and _table_suffix between start_date and end_date
            and totals.visits = 1
        group by 1, 2, 3, 4
    ), 
consolidation as (
select
    date,
    upper(country) as country,
    channel,
    skincare,
    fragrance,
    makeup,
    hair,
    addtocart,
    searches,
    list_scroll,
    sessions
from data
union all 
 select  
  date , 
  'FR' as country , 
   channel_grouping as channel, 
   skincare, 
   fragrance, 
   makeup, 
   hair,
   addtocart,
   0 as searches ,
   0 as list_scroll, 
   sessions
    from {{ ref('stg_piano_data') }}

)

select * from consolidation
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
