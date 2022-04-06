{{ config(materialized='table') }}

with date_range as (
select
    '20190101' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date 
    ) ,

-- Get all transactions from cbs reporting
cy_transaction_data as (
    SELECT
      Transaction_Date_Local,
       IF (Sales_Channel="null", "unidentified", Sales_Channel) AS device_cs,
        LOWER( IF
            (Merchant_Defined_Data12 IS NOT NULL,
            Merchant_Defined_Data12,
            CASE
                WHEN Merchant_ID IN ("sephora_fr", "sephora_fr_cc") THEN "FR"
                WHEN Merchant_ID IN ("sephora_it") THEN "IT"
                WHEN Merchant_ID IN ("sephora_se") THEN "SE"
                WHEN Merchant_ID IN ("sephora_dk") THEN "DK"
                WHEN Merchant_ID IN ("sephora_ge","sephora_ge_cc") THEN "DE"
                WHEN Merchant_ID IN ("sephora_ro","sephora_ro_cc") THEN "RO"
                WHEN Merchant_ID IN ("sephora_gr") THEN "GR"
                WHEN Merchant_ID IN ("sephora_gr_cc") THEN "GR"
            ELSE "error" END )) AS country,
      Merchant_ID,
      Merchant_Reference_Number AS TransactionId,
    FROM
       {{ source('cy_data', 'cbs_reports_PART') }} 
    WHERE
      ---Transaction_Date_Local = DATE_SUB(catchup_date, INTERVAL 0 day)
     --- and Sales_Channel is not null 
       Transaction_Date_Local >= date_trunc(current_date-365, year)
      AND Active_Profile_Decision IN ("ACCEPT", "REVIEW")
    GROUP BY 1,2,3,4,5

) , 

--- ga data 

ga as (

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
        COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))) AS sessions,
        COUNT(DISTINCT h.transaction.transactionId) AS transactions,
        round(sum( h.transaction.transactionRevenue/1000000),2) as revenue_local ,

      FROM
         {{ source('ga_data', 'ga_sessions_*') }} ,
        UNNEST (hits) AS h,
        date_range
      WHERE
        REGEXP_CONTAINS(h.page.hostname, r'^(www|m)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk|sa|ae|com.kw|om|bh|qa|ro|gr)$') IS TRUE 
        and _TABLE_SUFFIX between start_date and end_date
        and totals.visits = 1
      GROUP BY 1, 2, 3,4

),

--- Consolidate Data from cbs reporting 
cs_raw as ( 
  SELECT
    PARSE_DATE('%Y%m%d', REPLACE(CAST (Transaction_Date_Local AS string), "-", "")) AS date,
     EXTRACT (WEEK(SUNDAY) from PARSE_DATE('%Y%m%d', REPLACE (CAST (Transaction_Date_Local AS string), "-", ""))) AS week_number,
    
   CASE
      WHEN  device_cs='mobile' THEN 'mobile'
      WHEN  device_cs='application' THEN 'app'
      WHEN  device_cs='web' THEN 'desktop'
      WHEN  device_cs='csc' THEN 'other'
      WHEN  device_cs is null THEN 'other'
    ELSE 'other' END as device_cs,
    country,   
    count (DISTINCT TransactionId) AS transactions, 
    from cy_transaction_data
    GROUP BY 1,2,3,4
) , 

--- Only select 
    cs_final as (  
    select 
    date, 
    left( cast(date as string),4) as year,
    country, 
    sum(transactions) as transactions
    from cs_raw
    where device_cs in ("desktop","mobile")
   group by 1,2,3 
   ), 


--) Merge cbs data with google analytics data to have sessions and sessions retrevied 
   final as ( 
  select cast(ga.year as string) as year, 
  ga.Date,
  ga.country,
  ga.channel,
  ga.sessions, 
  ga.transactions,
  ga.revenue_local,
  SUM(ga.transactions) OVER (PARTITION BY ga.Date, ga.country) total_transactions,
  round(ga.transactions/ SUM(ga.transactions) OVER (PARTITION BY ga.Date, ga.country),4) as poids_transactions_total,
  cs_final.transactions as cs_transactions,
  round( cs_final.transactions*(round(ga.transactions/ SUM(ga.transactions) OVER (PARTITION BY ga.Date, ga.country),3)),0) as cs_transactions_pondere, 
  round( ga.transactions/ga.sessions,3) as conv_rate,
   FROM ga
   left join  cs_final
   on ga.Date=cs_final.date
   and ga.country=cs_final.country
   and cast(ga.year as string) = cs_final.year
   
   -- where ga.country in ('ro') --- a filtrer par pays si besoin 
   where ga.transactions >0

   )

   select  
   year,
   Date,
   upper(country) as country,
   channel,
   'website' as platform,
   sessions,
   case
   when conv_rate<=0.0 then sessions   ----si conv_rate<=0, alors pas de rattrapge de sessions (sinon erreur de calcul à cause de division sur 0)
   when cs_transactions_pondere <=0.0 then sessions   ---si cs_transactions_ponderée<=0, alors pas de rattrapge de sessions (le calcul ne fonctionnera pas)
   else round( cs_transactions_pondere/if(conv_rate<=0.0,1,conv_rate),0) --- sinon calcul de rattrapage de sessions
   end AS  sessions_rattrapee,
   transactions,
   revenue_local
   FROM final
   order by year desc , Date desc 



   
