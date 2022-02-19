{{ config(materialized='table') }}

-- Consolidation de toutes les sessions via la Roll Up Property '136443498'

SELECT
        date,
        -- CASE WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www|m)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk|sa|ae|com.kw|om|bh|qa|ro|gr)$") THEN "website"ELSE "empty" END AS datasource,
        -- CASE WHEN device.deviceCategory='mobile' THEN 'mobile' WHEN device.deviceCategory='desktop' THEN 'desktop' WHEN device.deviceCategory='tablet' THEN 'desktop' ELSE "empty"END  AS device,
        CASE
          WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www|m|api)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk)$") THEN LOWER(SUBSTR(h.sourcePropertyInfo.sourcePropertyDisplayName,9,2))
          WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www)\.sephora\.(ae)$") THEN "me"
       WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www)\.sephora\.(sa)$") THEN "me"
          WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www)\.sephora\.(om)$") THEN "me"
          WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www)\.sephora\.(bh)$") THEN "me"
          WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www)\.sephora\.(com.kw)$") THEN "me"
          WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www)\.sephora\.(qa)$") THEN "me"
          WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www)\.sephora\.(ro)$") THEN "ro"
          WHEN REGEXP_CONTAINS(h.page.hostname, r"^(www)\.sephora\.(gr)$") THEN "gr"
        ELSE
        "empty"
      END
        AS country,
        channelGrouping AS channel,
        COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))) AS sessions,
      FROM
         {{ source('ga_data', 'ga_sessions_*') }} ,
        UNNEST (hits) AS h
      WHERE
        REGEXP_CONTAINS(h.page.hostname, r"^(www|m)\.sephora\.(fr|de|pt|es|it|pl|se|cz|dk|sa|ae|com.kw|om|bh|qa|ro|gr)$") IS TRUE 
        and _TABLE_SUFFIX >= cast(DATE_TRUNC(current_date-10, Month) as string)
        AND totals.visits = 1
      GROUP BY 1, 2, 3