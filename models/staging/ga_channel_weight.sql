{{ config(materialized='table') }}


    SELECT
      *,
      SUM(sessions) OVER (PARTITION BY channel, country) AS channel_total,
      SUM(sessions) OVER(PARTITION BY country) AS total
      from {{ref('ga_global')}} 