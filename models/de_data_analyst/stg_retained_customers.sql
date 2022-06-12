
{{
  config(
    materialized = 'table',
    labels = {'type': 'crm_database', 'contains_pie': 'no', 'category':'analysis'}  )
}}


SELECT 
     week, 
     user_id, 
     customer_order_sequence,
     previous_order_week,
     week - previous_order_week as weekBetweenVisits,
     CASE WHEN ( week - previous_order_week )= 1 THEN 'retained'
      WHEN ( week - previous_order_week )> 1 THEN 'lagger'
      WHEN ( week - previous_order_week ) IS NULL THEN 'new'
     END as userType

FROM (

SELECT 
   week,
   user_id,

   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY 
   week ASC) as customer_order_sequence,

   LAG(week) OVER (PARTITION BY user_id ORDER BY 
   week ASC) as previous_order_week,

   FROM `valorissimo.raw_data.user_trafic_weekly`
   GROUP BY week, user_id 
   ORDER BY user_id, week)