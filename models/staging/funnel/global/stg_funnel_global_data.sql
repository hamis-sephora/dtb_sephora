{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

with date_range as (
select
    '20200101' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date 
    ) 

select 
  distinct
*
from {{ source('funnel', 'funnel_data_*') }} a ,
date_range
left join {{ source('funnel', 'funnel_account_orga') }} b 
on a.Data_Source_name = b.NAME
 where _table_suffix between start_date and end_date
and Country_ is not null 





