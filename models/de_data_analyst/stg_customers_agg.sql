
{{ config(materialized='table') }}

 SELECT 
     cardCode , 
     count (distinct accountType) as accountType,
     count(distinct Ticket_ID) as transactions, 
     count (distinct article_code) as distinct_article, 
     round(sum(cast(Sales_Ex_VAT as float64)),2) as revenue,
     min(cast(ticket_date as date)) as first_date, 
     max(cast(ticket_date as date)) as max_date, 
     date_diff(current_date() , min(cast(ticket_date as date)) , day) as customer_seniority_day, 
     date_diff(current_date() , max(cast(ticket_date as date)) , day) as customer_recency_day, 
     date_diff(current_date() , min(cast(ticket_date as date)) , month) as customer_seniority_month, 
     date_diff(current_date() , max(cast(ticket_date as date)) , month) as customer_recency_month,   
     {% for Axe_Desc in ['Other', "MakeUp", " Skincare" , "Hair", "Fragrance"]%}
     sum(case when Axe_Desc = '{{Axe_Desc}}' then cast(Sales_Ex_VAT as float64) end) as {{Axe_Desc}}_amount,
    {% endfor %}
    FROM {{ source('de', 'de_database') }} 
    group by 1