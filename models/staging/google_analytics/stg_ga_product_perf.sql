{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'staging'}  
  )
}}
with date_range as (
select
    '20200101' as start_date,
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
        p.productBrand, 
        p.v2ProductCategory as productCategory, 
        SPLIT(p.v2ProductCategory, '/')[OFFSET(0)] as category,        
        p.v2ProductName, 
        round(p.productPrice/1000000,2) as productPrice, 
        p.productVariant, 
         channelGrouping AS channel,
        COUNT(DISTINCT h.transaction.transactionId) AS transactions,
       round(sum( h.transaction.transactionRevenue/1000000),2) as revenue_local ,
      FROM
         `s4a-ga-raw-data-prd`.`136443498`.`ga_sessions_*` ,
        UNNEST (hits) AS h,
        UNNEST(product) as p,
        date_range
      WHERE
        REGEXP_CONTAINS(h.page.hostname, r'^(www|m)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk|sa|ae|com.kw|om|bh|qa|ro|gr)$') IS TRUE 
        and _TABLE_SUFFIX between start_date and end_date 
        and totals.visits = 1 and h.ecommerceaction.action_type = '6'
      GROUP BY 1, 2, 3,4 , 5, 6,7,8,9,10
) 

select
       Date , 
       year, 
       upper(country) as country , 
       channel, 
       round(sum(case when category = 'skincare' then productPrice end ),2) as skincare_revenue, 
       round(sum(case when category = 'makeup' then productPrice end ),2) as makeup_revenue, 
       round(sum(case when category = 'fragrance' then productPrice end ),2) as fragrance_revenue, 
       round(sum(case when category = 'hair' then productPrice end ),2) as hair_revenue, 
     from data 
     group by 1, 2, 3, 4

      