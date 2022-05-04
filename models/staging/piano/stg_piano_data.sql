{{
  config(
    materialized = 'table',
    labels = {'type': 'piano_analytics', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

SELECT
  date,
  channel_grouping,
  COUNT(DISTINCT VISIT_ID) AS sessions,
  SUM(TRANSACTION) AS TRANSACTION,
  SUM(revenue) AS revenue
FROM (
  SELECT
    date,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(IFNULL( IF (SRC_CHANNEL_GROUPING LIKE '%nic and%', SRC, SRC_CHANNEL_GROUPING), SRC)), r'mail') THEN 'Emailing'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL(
        IF
          (SRC_CHANNEL_GROUPING LIKE '%nic and%',
            SRC,
            SRC_CHANNEL_GROUPING),
          SRC)), r'social') THEN 'Social'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL( IF (SRC_CHANNEL_GROUPING LIKE '%nic and%', SRC, SRC_CHANNEL_GROUPING), SRC)), r'media') AND NOT REGEXP_CONTAINS(LOWER(IFNULL( IF (SRC_CHANNEL_GROUPING LIKE '%nic and%', SRC, SRC_CHANNEL_GROUPING), SRC)), r'social') THEN 'Media'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL(
        IF
          (SRC_CHANNEL_GROUPING LIKE '%nic and%',
            SRC,
            SRC_CHANNEL_GROUPING),
          SRC)), r'refer') THEN 'Referral'
      WHEN LOWER(IFNULL( IF (SRC_CHANNEL_GROUPING LIKE '%nic and%', SRC, SRC_CHANNEL_GROUPING), SRC)) = 'paid search non brand' THEN 'Paid Search Non Brand'
      WHEN LOWER(IFNULL(
      IF
        (SRC_CHANNEL_GROUPING LIKE '%nic and%',
          SRC,
          SRC_CHANNEL_GROUPING),
        SRC)) = 'paid search brand' THEN 'Paid Search Brand'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL( IF (SRC_CHANNEL_GROUPING LIKE '%nic and%', SRC, SRC_CHANNEL_GROUPING), SRC)), r'eprm') THEN 'Eprm'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL(
        IF
          (SRC_CHANNEL_GROUPING LIKE '%nic and%',
            SRC,
            SRC_CHANNEL_GROUPING),
          SRC)), r'organic')
    OR REGEXP_CONTAINS(LOWER(IFNULL(
        IF
          (SRC_CHANNEL_GROUPING LIKE '%nic and%',
            SRC,
            SRC_CHANNEL_GROUPING),
          SRC)), r'search engines') THEN 'Organic Search'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL( IF (SRC_CHANNEL_GROUPING LIKE '%nic and%', SRC, SRC_CHANNEL_GROUPING), SRC)), r'target') THEN 'Retargeting'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL(
        IF
          (SRC_CHANNEL_GROUPING LIKE '%nic and%',
            SRC,
            SRC_CHANNEL_GROUPING),
          SRC)), r'affili') THEN 'Affiliation'
      WHEN REGEXP_CONTAINS(LOWER(IFNULL( IF (SRC_CHANNEL_GROUPING LIKE '%nic and%', SRC, SRC_CHANNEL_GROUPING), SRC)), r'direct traffic') THEN 'Direct'
    ELSE
    '(Other)'
  END
    AS channel_grouping,
    VISIT_ID AS VISIT_ID,
    SUM(
    IF
      (EVENT_NAME='transaction.confirmation',
        1,
        0)) AS TRANSACTION,
    IFNULL(MAX(visit_sales),
      0) AS revenue
    ---FROM `s4a-ga-raw-data-prd.piano.raw_export_partitioned`
  FROM
    `s4a-ga-raw-data-prd.piano.events_daily_export`
    --WHERE date between parse_date('%Y%m%d',  @DS_START_DATE) and parse_date('%Y%m%d',@DS_END_DATE)
    --where date = date_sub(current_date(), interval 1 day)
  GROUP BY
    1,2,3)
GROUP BY 1, 2