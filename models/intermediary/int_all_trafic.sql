{{
    config(
        materialized='table',
        labels={
            'type': 'google_analytics',
            'contains_pie': 'no',
            'category': 'reporting',
        },
    )
}}

/* Données Google Analytics, les données de coûts sont en dollars et convertis en euro*/
with
    data as (
        select
            date as date,
            extract(week(sunday) from date) as week,
            case when country in ('DK','SE') then 'SCANDI' else country end as country,
            channel,
            platform,
            sessions,
            addtocart,
            sessions_rattrapee,
            transactions,
            revenue_local,
            revenue_local / 0.95 as revenue_euro
        from {{ ref('stg_sessions_retrieved_channel') }}

        union all
        /* Données cybersource , les données de coût sont déjà convertis en euros*/
        select
            date,
            extract(week(sunday) from date) as week,
            case when country in ('DK','SE') then 'SCANDI' else country end as country,
            channel,
            'app' as platform,
            sum(sessions) as sessions,
            sum(addtocart) as addtocart,
            0 as sessions_rattrapee,
            sum(transactions) as transactions,
            sum(revenue_local) as revenue_local,
            sum(revenue_euro) as revenue_euro,
        from {{ ref('stg_ga4_global') }}
        group by 1, 2, 3, 4, 5

        union all
        /* Données cybersource , les données de coût sont déjà convertis en euros*/
        select
            date,
            extract(week(sunday) from date) as week,
            case when country in ('DK','SE') then 'SCANDI' else country end as country,
            'organic' as channel,
            datasource_cs as platform,
            sum(sessions) as sessions,
            0 as addtocart,
            0 as sessions_rattrapee,
            sum(transactions) as transactions,
            sum(revenue_local) as revenue_local,
            sum(revenue_euro) as revenue_euro
        from {{ ref('stg_global_cybersource') }}
        where datasource_cs = 'website' and country = 'RU'
        group by 1, 2, 3, 4, 5

        union all

        /* Données GR dispo dans le compte GA dédié à GR, donnée de coût en euro */
        select
            parse_date('%Y%m%d', cast(date as string)) as date,
            extract(
                week(sunday) from parse_date('%Y%m%d', cast(date as string))
            ) as week,
            country,
            channel,
            'website' as platform,
            sessions as sessions,
            0 as addtocart,
            0 as ssessions_rattrapee,
            transactions as transactions,
            revenue as revenue_local,
            revenue as revenue_euro,
        from `s4a-digital-dwh-prd.eme_media_data.gr_historic_data`
        where parse_date('%Y%m%d', cast(date as string)) < '2021-10-21'

        union all
        /* Données RO dispo dans le compte GA dédié , donnée de coût converti en euro */
        select
            parse_date('%Y%m%d', cast(date as string)) as date,
            extract(
                week(sunday) from parse_date('%Y%m%d', cast(date as string))
            ) as week,
            country,
            channel,
            'website' as platform,
            sessions as sessions,
            0 as addtocart,
            0 as ssessions_rattrapee,
            transactions as transactions,
            revenue as revenue_local,
            round(revenue / 4.895, 2) as revenue_euro,
        from `s4a-digital-dwh-prd.eme_media_data.ro_historic_data`
        where parse_date('%Y%m%d', cast(date as string)) < '2021-09-21'

        union all 
        /* Données TR dispo dans le compte GA dédié , donnée de coût converti en euro */
        select
            parse_date('%Y%m%d', cast(date as string)) as date,
            extract(
                week(sunday) from parse_date('%Y%m%d', cast(date as string))
            ) as week,
            country,
            Default_Channel_Grouping__Regroupement_de_canaux_par_d_____faut_ as channel,
            'website' as platform,
            sessions as sessions,
            0 as addtocart,
            0 as ssessions_rattrapee,
            transactions as transactions,
            Revenu as revenue_local,
            round(Revenu / 16.8, 2) as revenue_euro,
        from `s4a-digital-dwh-prd.eme_media_data.tr_historic_data`
        where parse_date('%Y%m%d', cast(date as string)) < '2022-04-22'        

        /*  -- Consolidation données Piano Analytics */
        union all 
       
        select 
           date, 
           extract(week(sunday) from date ) as week, 
           'FR' as country, 
           channel_grouping,
           'website' as platform,  
           sessions, 
           addtocart as addtocart, 
           0 as sessions_rattrapee,
           transaction as transactions, 
           revenue,
           revenue as revenue_euro
        from {{ ref('stg_piano_data') }}  
        where date > '2022-05-03'
    

    )
/* Consolidation globale des données */
select
    date,
    extract(week(sunday) from date) as week,
    country,
    channel,
    case
        when
            channel in (
                'Direct', 'Organic Search', 'Referral','app',  '(Other)', 'App', 'organic'
            )
        then 'ORGANIC'
        when channel in ('Emailing', 'Eprm')
        then 'OWN MEDIA'
        else 'PAID MEDIA'
    end as channel_grouping,
    platform,
    sessions,
    addtocart,
    sessions_rattrapee,
    case
        when
            platform = 'website' and country not in ('RU', 'ME') and date between '2021-10-01' and '2022-05-03'
        then sessions_rattrapee
        else sessions
    end as sessions_retrieved,
    transactions,
    revenue_local,
    revenue_euro,
from data
order by date desc 


