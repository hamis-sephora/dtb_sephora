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
    case when length(month) = 1 then concat('0', month) else month end as month,
    concat(
        year,case when length(month) = 1 then concat('0', month) else month end,'01'
    ) as date,
    year,
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
from {{ source('cac_mvp', 'MVP_BY_Months_Channel_Support_BQ') }}
    ),

  data_crm_cons as (
    select 
      country , 
      parse_date('%Y%m%d', cast(date as string)) as month, 
      channel_grouping,
      channel,
      concat(country,'_',parse_date('%Y%m%d', cast(date as string)),'_',channel) as ligne_id,
     sum(nb_new_estore_clients) as nb_new_estore_clients,
     sum(nb_new_sephora_clients) as nb_new_sephora_clients, 
     sum(nb_clients) as nb_clients, 
     sum(nb_of_visits) as nb_of_visits, 
     sum(revenue) as revenue, 
     sum(new_to_sephora_revenue) as new_to_sephora_revenue,
     sum(numberoftransaction) as numberoftransaction
     from data_crm 
     group by 1,2,3,4,5
     order by month desc
  ),

    media_data as (
        select
            concat(substr(cast(date as string),0,7),'-01') as month,
            country,
            concat(country,'_',concat(substr(cast(date as string),0,7),'-01'),'_',channel_grouping) as ligne_id,
            channel_grouping as channel,
            sum(cost) as cost,
            sum(impressions) as impressions,
            sum(clicks) as clicks
        from {{ ref('stg_funnel_eme_reporting') }}
        group by 1,2,3,4
        order by month desc 
    )
select
    data_crm_cons.country,
    data_crm_cons.month,
    data_crm_cons.channel as channel , 
    data_crm_cons.channel_grouping,
    media_data.channel as media_channel_grouping,     
    nb_new_estore_clients,
    nb_new_sephora_clients,
    new_to_sephora_revenue,
    nb_clients,
    nb_clients-nb_new_sephora_clients as nb_old_to_sephora,
    revenue-new_to_sephora_revenue as old_to_sephora_revenue,
    nb_of_visits,
    revenue,
    numberoftransaction,
    media_data.clicks,
    media_data.impressions,
    media_data.cost
from data_crm_cons
left join
    media_data on data_crm_cons.ligne_id = media_data.ligne_id
               and data_crm_cons.country = media_data.country
order by data_crm_cons.month desc 

