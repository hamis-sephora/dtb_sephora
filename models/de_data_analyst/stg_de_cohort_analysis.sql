{{
  config(
    materialized = 'table',
labels = {'type': 'crm_database', 'contains_pie': 'no', 'category':'analysis'}  )
}}

WITH t_first_purchase AS (
  SELECT 
  Date,
  DATE_DIFF(Date, first_purchase_date, MONTH) AS month_order,
  FORMAT_DATETIME('%Y%m', first_purchase_date) AS first_purchase,
  cardCode
  FROM (
    SELECT 
    DATE(TIMESTAMP(ticket_date)) AS date,
    cardCode,
    FIRST_VALUE(DATE(TIMESTAMP(ticket_date))) OVER (PARTITION BY cardCode ORDER BY DATE(TIMESTAMP(ticket_date))) AS first_purchase_date
    FROM  {{ source('de', 'de_data_crm') }} 
        where store_code = '2487'   -- E-Store Only

    )
  ),

/* This table computes the aggregate customer count per first purchase cohort and month order */
t_agg AS (
  SELECT 
  first_purchase,
  month_order,
  COUNT(DISTINCT cardCode) AS Customers
  FROM 
  t_first_purchase
  GROUP BY first_purchase, month_order
),

/* This table computes the retention rate */
 t_cohort AS (
  SELECT *,
  SAFE_DIVIDE(Customers, CohortCustomers) AS CohortCustomersPerc
  FROM (
      SELECT *,
      FIRST_VALUE(Customers) OVER (PARTITION BY first_purchase ORDER BY month_order) AS CohortCustomers
      FROM t_agg
  )
 )

SELECT * FROM t_cohort 
ORDER BY first_purchase, month_order