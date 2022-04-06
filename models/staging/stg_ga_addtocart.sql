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
        count(distinct concat(fullVisitorId, cast(visitStartTime AS STRING))) AS sessions

      FROM
          {{ source('ga_data', 'ga_sessions_*') }} ,
         date_range,
        UNNEST (hits) AS h
      WHERE
        REGEXP_CONTAINS(h.page.hostname, r'^(www|m)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk|sa|ae|com.kw|om|bh|qa|ro|gr)$') IS TRUE 
        and _TABLE_SUFFIX between start_date and end_date
        AND h.eCommerceAction.action_type = '3'
      GROUP BY 1, 2, 3, 4)
select 
       Date, 
       upper(country) as country, 
       channel , 
       case when channel in ('Paid Search Non Brand', 'Paid Search Brand','Social', 'Retargeting', 'media') then 'PAID MEDIA'
           when channel in ('Emailing') then 'OWN MEDIA'
           else 'ORGANIC'
           end as channel_grouping,  
       platform,
       0 as sessions , 
       0 as sessions_rattrapee,
       0 as sessions_retrieved,
       0 as transactions,
       0 as revenue_local,
       0 as revenue  , 
       sessions as session_addtocart,       
       from data




       