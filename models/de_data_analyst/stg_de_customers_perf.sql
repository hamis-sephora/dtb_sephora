{{ config(materialized='table') }}

with transaction_data as (
select 
    ticket_date, 
    count( distinct Ticket_ID) as transactions, 
    count( distinct cardCode) as users, 
    round(sum(cast(Sales_Ex_VAT as float64)),2) as revenue,
    count (distinct case when customer_type = 'New Customer' then cardCode end ) as new_customers,
    count (distinct case when customer_type = 'Old Costomer' then cardCode end ) as old_customers,
    round(sum (case when customer_type = 'New Customer' then cast(Sales_Ex_VAT as float64) end ),2) as new_customers_revenue,
    round(sum (case when customer_type = 'Old Costomer' then cast(Sales_Ex_VAT as float64) end ),2) as old_customers_revenue
    from {{ref('stg_de_new_customers')}} 
    group by 1
) , 
trafic_data as (
    SELECT 
      date , 
      sum(sessions_retrieved) as sessions, 
      sum(case when platform = 'App' then sessions_retrieved end) as session_app, 
      sum(case when platform = 'website' then sessions_retrieved end) as session_website, 
      round(sum(revenue),2) as revenue,
      sum(case when platform = 'App' then revenue end) as revenue_app,     
      round(sum(case when platform = 'website' then revenue end),2) as revenue_website , 
      sum(transactions) as transactions, 
      sum(case when platform = 'App' then transactions end) as transactions_app,     
      sum(case when platform = 'website' then transactions end) as transactions_website ,     
 FROM {{ref('prd_all_trafic')}} 
 where country = 'DE'
 group by 1 
 order by date asc 
), 
cost_data as (
select 
     date , 
     country , 
     sum(impressions) as impressions, 
     sum(clicks) as clicks,
     sum(case when channel = 'Social' then clicks end) as clicks_social,
     sum(case when channel = 'Search' then clicks end) as clicks_search,
     sum(final_costs) as cost,
     sum(case when channel = 'Social' then final_costs end) as cost_social,
     sum(case when channel = 'Search' then final_costs end) as cost_search,     
     from {{ref('stg_global_cost')}} 
     where country = 'DE'
     group by 1,2
)

select ticket_date, 
       transaction_data.transactions as transactions_crm,
       users as customers, 
       transaction_data.revenue as revenue_crm, 
       new_customers, 
       old_customers,
       new_customers_revenue,
       old_customers_revenue, 
       sessions , 
       session_app,
       session_website,
       trafic_data.revenue as revenue_ga,
       revenue_app, 
       revenue_website, 
       trafic_data.transactions as transactions_ga, 
       transactions_app,
       transactions_website, 
       impressions, 
       clicks_social,
       clicks_search,
       cost,
       cost_social,
       cost_search
       from transaction_data 
   left join trafic_data
       on transaction_data.ticket_date = cast(trafic_data.date as string)
   left join cost_data 
       on transaction_data.ticket_date = cast(cost_data.date as string)