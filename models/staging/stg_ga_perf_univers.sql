{{ config(materialized='table') }}

with date_range as (
select
    '20210101' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date 
    ) , 
data as (
SELECT
        PARSE_DATE('%Y%m%d', date) AS Date,
        EXTRACT (year from PARSE_DATE('%Y%m%d', date)) AS year,
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
        count (distinct case when h.eCommerceAction.action_type = '3'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end ) as addtocart, 
        count (distinct case when h.eventInfo.eventCategory = 'search'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end ) as searches,     
        count (distinct case when h.eventInfo.eventCategory = 'product list' and h.eventInfo.eventAction = 'list scroll' then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end ) as list_scroll,         
        count (distinct case when h.contentGroup.contentGroup1 = 'skincare'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end ) as skincare, 
        count (distinct case when h.contentGroup.contentGroup1 = 'fragrance'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) end	) as fragrance, 
        count (distinct case when h.contentGroup.contentGroup1 = 'makeup'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end) as makeup, 
        count (distinct case when h.contentGroup.contentGroup1 = 'hair'then CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))	end) as hair,        
        COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))) AS sessions,
        --COUNT(DISTINCT h.transaction.transactionId) AS transactions,
        --round(sum( h.transaction.transactionRevenue/1000000),2) as revenue_local ,
      FROM
         {{ source('ga_data', 'ga_sessions_*') }} ,
        UNNEST (hits) AS h,
        date_range
      WHERE
        REGEXP_CONTAINS(h.page.hostname, r'^(www|m)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk|sa|ae|com.kw|om|bh|qa|ro|gr)$') IS TRUE 
        and _TABLE_SUFFIX between start_date and end_date 
        and totals.visits = 1
      GROUP BY 1, 2, 3,4) 

      select 
            Date , 
            upper(country) as country , 
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
          