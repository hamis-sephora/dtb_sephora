{{
    config(
        materialized='table',
        labels={'type': 'crm_database', 'contains_pie': 'no', 'category': 'analysis'},
    )
}}
with
    data_crm as (
           select
    country,
    concat(
        year,'-',case when length(week) = 1 then concat('0', week) else week end ) as week,
    year,
    week as week_n, 
    channel,
    case
        when
            channel in (
                'Direct', 'Organic Search', 'Referral', '(Other)', 'App', 'organic'
            )
        then 'ORGANIC'
        when channel in ('Emailing', 'Eprm')
        then 'OWN MEDIA'
        else 'PAID MEDIA'
    end as channel_grouping,
    nb_new_estore_clients,
    nb_new_sephora_clients,
    pct_new_to_sephora,
    nb_clients,
    nb_of_visits,
    pct_of_visits_with_cardcode,
    revenue,
    new_to_sephora_revenue,
    numberoftransaction
from {{ source('cac_mvp', 'MVP_BY_Week_Channel_Support_BQ') }}
where country = 'DE'
    ),

  data_crm_cons as (
    select 
      country , 
      year,
      week, 
      week_n, 
      channel_grouping,
      channel,
      concat(country,'_',week,'_',channel) as ligne_id,
     sum(nb_new_estore_clients) as nb_new_estore_clients,
     sum(nb_new_sephora_clients) as nb_new_sephora_clients, 
     sum(nb_clients) as nb_clients, 
     sum(nb_of_visits) as nb_of_visits, 
     sum(revenue) as revenue, 
     sum(new_to_sephora_revenue) as new_to_sephora_revenue,
     sum(numberoftransaction) as numberoftransaction
     from data_crm 
     group by 1,2,3,4,5,6,7
  ),

media_data as (

        select
            date,
            extract(year from date) as year , 
            extract( week(sunday) from date ) as week , 
            case when length(cast(extract( week(sunday) from date ) as string)) = 1 then concat('0', extract( week(sunday) from date)) else cast(extract( week(sunday) from date ) as string) end as week_y , 
            country,
            concat(country,'_',concat(substr(cast(date as string),0,7),'-01'),'_',channel_grouping) as ligne_id,
            channel_grouping as channel,
            sum(cost) as cost,
            sum(impressions) as impressions,
            sum(clicks) as clicks
        from {{ ref('stg_funnel_eme_reporting') }}
        where country = 'DE'
        group by 1,2,3,4,5,6,7) , 

media_cons as (
        select 
              country , 
              year, 
              week_y as week, 
              concat( country ,'_',concat(year,'-',week_y),'_', channel) as ligne_id, 
             channel,
             sum(cost) as cost,
             sum(impressions) as impressions,
             sum(clicks) as clicks
             from media_data
             group by 1,2,3,4,5

    )
select
    data_crm_cons.country,
    data_crm_cons.year,
    data_crm_cons.week,    
    data_crm_cons.week_n,        
    data_crm_cons.channel as channel , 
    data_crm_cons.channel_grouping,
    media_cons.channel as media_channel_grouping,     
    nb_new_estore_clients,
    nb_new_sephora_clients,
    new_to_sephora_revenue,
    nb_clients,
    nb_clients-nb_new_sephora_clients as nb_old_to_sephora,
    revenue-new_to_sephora_revenue as old_to_sephora_revenue,
    nb_of_visits,
    revenue,
    numberoftransaction,
    media_cons.ligne_id as media_ligne_id, 
    media_cons.clicks,
    media_cons.impressions,
    media_cons.cost
from data_crm_cons
left join
    media_cons on data_crm_cons.ligne_id = media_cons.ligne_id



