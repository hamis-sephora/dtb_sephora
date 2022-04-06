{{ config(materialized='table') }}

with site_centric as (
SELECT
  week as week_number,
  country,
  sum(sessions_2021) as sessions_2021,
  sum(sessions_2022) as sessions_2022,
  sum(conversions_2021) as conversions_2021,
  sum(conversions_2022) as conversions_2022,
  round(sum(revenue_2021),2) as revenue_2021,
  round(sum(revenue_2022),2) as revenue_2022,
  sum(sessions_rattrapee_2022) as sessions_rattrapee_2022,
  sum(sessions_forecast) as sessions_forecast
FROM
 {{ref('int_report_session')}} 
where week > 0   
group by 1,2 
) , 
 
 ad_centric as (
  SELECT
  week_number,
  country,
  round(sum(cost_2021),2) as cost_2021,
  round(sum(cost_2022),2) as cost_2022
FROM
 {{ref('int_media')}} 
group by 1,2
order by week_number asc, country asc   
)
select 
     site_centric.week_number ,
     site_centric.country , 
     sessions_2021 , 
     sessions_2022 ,
     conversions_2021, 
     conversions_2022, 
     revenue_2021, 
     revenue_2022, 
     sessions_rattrapee_2022, 
     sessions_forecast, 
     cost_2021, 
     cost_2022 , 
     round(SAFE_DIVIDE(cost_2021, revenue_2021),2) as cac_2021,
     round(SAFE_DIVIDE(cost_2022, revenue_2022),2) as cac_2022,
     round(SAFE_DIVIDE(cost_2021, conversions_2021),2) as cpo_2021,
     round(SAFE_DIVIDE(cost_2022 , conversions_2022),2) as cpo_2022,
     round(SAFE_DIVIDE(cost_2021 , sessions_2021),2) as cpv_2021,
     round(SAFE_DIVIDE(cost_2022 , sessions_2022),2) as cpv_2022

    from site_centric
    left join ad_centric
    on site_centric.week_number = ad_centric.week_number and site_centric.country = ad_centric.country











