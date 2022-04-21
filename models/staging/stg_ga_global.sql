{{ config(materialized='table') }}
-- Trafic global sites internet tout pays ( source : vue roll up property )
with date_range as (
select
    '20190101' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date 
    ) ,

/*WEEK : renvoie le numéro de semaine de la date (compris dans la plage [0, 53])
Les semaines commencent le dimanche et les dates antérieures au premier dimanche 
de l'année correspondent à la semaine 0
*/

data as (

SELECT
        PARSE_DATE('%Y%m%d',date) as Date, 
        CASE
          WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www|m|api)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk)$') THEN LOWER(SUBSTR(h.sourcePropertyInfo.sourcePropertyDisplayName,9,2))
          WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www)\.sephora\.(ae)$') THEN 'me'
       WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www)\.sephora\.(sa)$') THEN 'me'
          WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www)\.sephora\.(om)$') THEN 'me'
          WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www)\.sephora\.(bh)$') THEN 'me'
          WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www)\.sephora\.(com.kw)$') THEN 'me'
          WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www)\.sephora\.(qa)$') THEN 'me'
          WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www)\.sephora\.(ro)$') THEN 'ro'
          WHEN REGEXP_CONTAINS(h.page.hostname, r'^(www)\.sephora\.(gr)$') THEN 'gr'
        ELSE 'empty'END AS country,
        channelGrouping AS channel,
        'website' as platform,
        device.deviceCategory	 as device, 
        count(distinct concat(fullVisitorId, cast(visitStartTime AS STRING))) AS sessions,
        count (distinct case when h.eCommerceAction.action_type = '3'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end ) as addtocart, 
        count (distinct case when h.eventInfo.eventCategory = 'search'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end ) as searches, 
        count (distinct case when h.eventInfo.eventCategory = 'product list' and h.eventInfo.eventAction = 'list scroll' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end ) as list_scroll,         
        count (distinct case when h.contentGroup.contentGroup1 = 'skincare'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end ) as skincare, 
        count (distinct case when h.contentGroup.contentGroup1 = 'fragrance'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end	) as fragrance, 
        count (distinct case when h.contentGroup.contentGroup1 = 'makeup'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end) as makeup, 
        count (distinct case when h.contentGroup.contentGroup1 = 'hair'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end) as hair,            
        count (distinct h.transaction.transactionId) as conversions,
        round(sum( h.transaction.transactionRevenue/1000000),2) as revenue_local , 
        round(sum( h.transaction.transactionRevenue/1000000)/  count (distinct h.transaction.transactionId),2) as aov,

      FROM
         {{ source('ga_data', 'ga_sessions_*') }} ,
         date_range,
        UNNEST (hits) AS h
      WHERE
        REGEXP_CONTAINS(h.page.hostname, r'^(www|m)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk|sa|ae|com.kw|om|bh|qa|ro|gr)$') IS TRUE 
        and _TABLE_SUFFIX between start_date and end_date
        AND totals.visits = 1
      GROUP BY 1, 2, 3, 4,5
      order by Date desc 
)
select 
       Date, 
       country, 
       channel, 
       platform,
       device, 
       sessions, 
       addtocart, 
       searches, 
       list_scroll, 
       skincare, 
       fragrance,
       makeup, 
       hair, 
       conversions ,
       revenue_local,
       case when country in ('pl', 'PL') then revenue_local/4.5 
        when country in ('cz', 'CZ') then revenue_local/26.75
        when country in ('ae', 'AE') then revenue_local/4.187
        when country in ('sa', 'SA') then revenue_local/4.275
        when country in ('dk','DK') then revenue_local/7.6
        when country in ('se', 'SE') then revenue_local/11
        when country in ('om', 'OM') then revenue_local/0.46
        when country in ('bh', 'BH') then revenue_local/0.46
        when country in ('kw', 'KW') then revenue_local/0.36
        when country in ('ru', 'RU', 'ru Sephora', 'ru IDB') then revenue_local/88.69
        when country in ('qa', 'QA') then revenue_local/4.385
        when country in ('ro', 'RO') then revenue_local/4.895
       else revenue_local end as revenue
       from data 







