{{ config(materialized='table') }}

WITH t_first_purchase AS (
  SELECT 
  Date,
  DATE_DIFF(Date, first_purchase_date, MONTH) AS month_order,
  FORMAT_DATETIME('%Y%m', first_purchase_date) AS first_purchase,
  revenue
  FROM (
    SELECT 
    DATE(TIMESTAMP(ticket_date)) AS date,
    cast(Sales_Ex_VAT as float64) as revenue,
    FIRST_VALUE(DATE(TIMESTAMP(ticket_date))) OVER (PARTITION BY cardCode ORDER BY DATE(TIMESTAMP(ticket_date))) AS first_purchase_date
    FROM  {{ source('de', 'de_database') }} 
    )
  ),

/* This table computes the aggregate customer count per first purchase cohort and month order */
t_agg AS (
  SELECT 
  first_purchase,
  month_order,
  sum(revenue)as revenue
  FROM 
  t_first_purchase
  GROUP BY first_purchase, month_order
),

/* This table computes the retention rate */
 t_cohort AS (
  SELECT *,
  SAFE_DIVIDE(revenue, CohortRevenue) AS CohortCustomersPerc
  FROM (
      SELECT *,
      FIRST_VALUE(revenue) OVER (PARTITION BY first_purchase ORDER BY month_order) AS CohortRevenue
      FROM t_agg
  )
 )

SELECT * FROM t_cohort 
ORDER BY first_purchase, month_order

