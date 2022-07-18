
{{
    config(
        materialized='table',
        labels={'type': 'crm_database', 'contains_pie': 'no', 'category': 'analysis'},
    )
}}

select
    cardcode,
    count(distinct ticket_id) as transactions,
    count(distinct article_code) as distinct_article,
    round(sum(cast(sales_ex_vat as float64)), 2) as revenue,
    min(cast(ticket_date as date)) as first_date,
    max(cast(ticket_date as date)) as max_date,
    date_diff(
        current_date(), min(cast(ticket_date as date)), day
    ) as customer_seniority_day,
    date_diff(
        current_date(), max(cast(ticket_date as date)), day
    ) as customer_recency_day,
    date_diff(
        current_date(), min(cast(ticket_date as date)), month
    ) as customer_seniority_month,
    date_diff(
        current_date(), max(cast(ticket_date as date)), month
    ) as customer_recency_month,
    {% for Axe_Desc in ['Other', 'MakeUp', ' Skincare', 'Hair', 'Fragrance'] %}
    sum(
        case when axe_desc = '{{Axe_Desc}}' then cast(sales_ex_vat as float64) end
    ) as {{ Axe_Desc }}_amount,
    {% endfor %}
from {{ source('de', 'de_data_crm') }}
group by 1
order by transactions desc 
