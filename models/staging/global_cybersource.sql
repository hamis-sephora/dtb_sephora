
{{ config(materialized='table') }}

SELECT
    date,
    LEFT(date,4) AS year,
    country,
    sessions,
    datasource_cs,
    CASE
      WHEN CONCAT(LEFT(date,4),datasource_cs) in ('2021app', '2022app') THEN 1
      WHEN LEFT(date,4) not in ('2021', '2022') THEN 1
    ELSE
    0
  END
    AS flag
  FROM
    {{ source('cy_data', 'cbs_ga_fb_aggregated') }} 