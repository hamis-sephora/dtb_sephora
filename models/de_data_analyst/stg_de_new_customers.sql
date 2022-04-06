{{ config(materialized='table') }}
with data as (
SELECT  
  cardCode,
  ticket_date,
  accountType,
  Ticket_ID,
  Sales_Ex_VAT,
  sales_vat,
  store_code,
  store_country,
  quantity
FROM {{ source('de', 'de_database') }} 
) , 

type_customers as (
SELECT  *,
    RANK() OVER (PARTITION BY cardCode ORDER BY cast(ticket_date as date)) AS customer_seq
    FROM data 
   order by cast(ticket_date as date)
    ) 
    
select 
  cardCode,
  ticket_date,
  accountType,
  Ticket_ID,
  Sales_Ex_VAT,
  sales_vat,
  store_code,
  store_country,
  quantity ,
 customer_seq,
 case when customer_seq = 1 then 'New Customer' else 'Old Costomer' end customer_type
 from type_customers
