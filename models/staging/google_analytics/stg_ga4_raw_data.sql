{{
  config(
    materialized = 'table',
    labels = {'type': 'google_analytics', 'contains_pie': 'no', 'category':'staging'}  
  )
}}
select 
  Date,
  Data_Source_type,
  Data_Source_id,
  Currency,
  Data_Source,
  Data_Source_name,
  SPLIT(SPLIT(Data_Source_name, '|')[OFFSET(0)],' ')[OFFSET(1)] as country,
  Month,
  Week,
  Year,
  Add_to_carts___GA4__Google_Analytics as add_to_cart, 
  Checkouts___GA4__Google_Analytics as checkout ,
  Ecommerce_purchases___GA4__Google_Analytics as purchases ,
  Item_views___GA4__Google_Analytics as product_view ,
  Purchase_revenue___GA4__Google_Analytics as revenue_euro,
  Transactions___GA4__Google_Analytics as transactions,
  Device_category___GA4__Google_Analytics as device ,
  Platform___GA4__Google_Analytics as platform,
  Total_revenue___GA4__Google_Analytics as total_revenue,
  case when Session_medium___GA4__Google_Analytics = '(other)' then 0 else Sessions___GA4__Google_Analytics end as sessions,
  Session_campaign___GA4__Google_Analytics as ga_campaign,
  Session_medium___GA4__Google_Analytics as ga_medium,
  Session_source___GA4__Google_Analytics as ga_source,
  Session_source__medium___GA4__Google_Analytics as ga_source_medium,
  Account_ID___UA__Google_Analytics as account_id,
  Property_ID___UA__Google_Analytics as property_id,
  Property_name___UA__Google_Analytics as property_name,
  Samples_read_rate___UA__Google_Analytics as samples_read_rate,
  View_ID___UA__Google_Analytics as view_id,
  View_name___UA__Google_Analytics as view_name,
  View_type___UA__Google_Analytics as view_type,
  Website_URL___UA__Google_Analytics as website_url
 from {{ source('funnel', 'funnel_ga4_data') }} 









