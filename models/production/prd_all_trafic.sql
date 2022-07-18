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

select 
       date, 
       week, 
       case when country in ('DK','SE', 'Scandi') then 'SCANDI' else country end as country,
       channel,
       channel_grouping ,
       platform, 
       sessions, 
       sessions_rattrapee, 
       sessions_retrieved, 
       addtocart, 
       transactions, 
       revenue_local, 
       revenue_euro
from {{ref('int_all_trafic')}} 
where country not like ('RU')
order by date desc 




















