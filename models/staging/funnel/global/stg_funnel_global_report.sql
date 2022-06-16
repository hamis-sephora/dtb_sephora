{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}



select  
      Date , 
      Country_, 
      Data_Source_type as regie_source, 
      Data_Source_name as source_name,
      campaign,
      media_type, 
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost ,
    sum(ifnull (All_Conv__Value__GA_Transaction_SephoraEUR_SR_TUR_EMEA_Search_Google_PML_TRY_LVMH____8073571566__AdWords,0) +
   ifnull (All_Conv__Value__SephoraEUR_Site_Sales_PurchaseConfirmation_Sales_Transaction_20192202_Site_Sale_SephoraEUR_SR_TUR_EMEA_Search,0) +
   ifnull (All_Conv__Value__Transactions_AE__1_1__Main_Profile___NEW_SephoraEUR_SR_UAE_EMEA_Search_Google_PML_USD_LVMH____1801457948__AdW,0)+
   ifnull (All_Conv__Value__Transactions_AE__1_2__Main_Profile___OLD_SephoraEUR_SR_UAE_EMEA_Search_Google_PML_USD_LVMH____1801457948__AdW,0) +
   ifnull (All_Conv__Value__Transactions_All_Web_Site_Data_SephoraEUR_SR_RUS_EMEA_Search_Google_PML_RUB_LVMH_Brand___9398486580__AdWords,0) +
   ifnull (All_Conv__Value__Transactions_CH__1_1__Main_Profile_EUR_SephoraEUR_SR_CH_EMEA_Search_Google_LAB_EUR_LVMH____6901394591__AdWord,0) +
   ifnull (All_Conv__Value__Transactions_All_Web_Site_Data_SephoraEUR_SR_RUS_EMEA_Search_Google_PML_RUB_LVMH_General___7358146252__AdWord,0)+
   ifnull (All_Conv__Value__Transactions_CZ__1_1__Main_Profile_Sephora___9953053214__AdWords,0)+
   ifnull (All_Conv__Value__Transactions_DE__1_1__Main_Profile_SephoraEUR_SR_DEU_EMEA_Search_Google_LAB_EUR_LVMH____8382445893__AdWords,0)+
   ifnull (All_Conv__Value__Transactions_DK__1_1__Main_Profile_SephoraEUR_SR_DNK_EMEA_Search_Google_LAB_EUR_LVMH____1114321413__AdWords,0)+
   ifnull (All_Conv__Value__Transactions_ES__1_1__Main_Profile_SephoraEUR_SR_ESP_EMEA_Search_Google_LAB_EUR_LVMH____4444976669__AdWords,0)+
   ifnull (All_Conv__Value__Transactions_ES__1_1__Main_Profile_SephoraEUR_SR_ESP_EMEA_Search_Google_LAB_EUR_LVMH_CSS___7683939009__AdWord,0)+
   ifnull (All_Conv__Value__Transactions_ES__1_1__Main_Profile_SephoraEUR_SR_ESP_EMEA_Search_Google_LAB_EUR_LVMH_YouTube___8018945072__Ad,0)+
   ifnull (All_Conv__Value__Transactions_ES___GTAG_SephoraEUR_SR_ESP_EMEA_Search_Google_LAB_EUR_LVMH_YouTube___8018945072__AdWords,0)+
   ifnull (All_Conv__Value__Transactions_FR__1_1__Main_Profile_SephoraEUR_SR_BEL_EMEA_Search_Google_LAB_EUR_LVMH____9439832600__AdWords,0)+
   ifnull (All_Conv__Value__Transactions_FR__1_1__Main_Profile_SephoraEUR_SR_FRA_EMEA_Search_Google_LAB_EUR_LVMH____7438136108__AdWords,0)+
  ifnull ( All_Conv__Value__Transactions_FR__1_1__Main_Profile_SephoraEUR_SR_FRA_EMEA_Search_Google_LAB_EUR_LVMH_CSS___4746983045__AdWord,0)+
  ifnull ( All_Conv__Value__Transactions_FR__1_1__Main_Profile_SephoraEUR_SR_FRA_EMEA_Search_Google_LAB_EUR_LVMH_CSS___4746983045__AdWord_1,0)+
   ifnull (All_Conv__Value__Transactions_FR__1_1__Main_Profile_SephoraEUR_SR_FRA_EMEA_Search_Google_LAB_EUR_LVMH_Trade_Marketing___902560,0)+
   ifnull (All_Conv__Value__Transactions_FR__1_1__Main_Profile_SephoraEUR_SR_FRA_EMEA_Search_Google_LAB_EUR_LVMH_Visibility___6943817893_,0)+
  ifnull ( All_Conv__Value__Transactions_GR__1_1__Main_Profile_SephoraEUR_SR_GRC_EMEA_Search_Google_PML_EUR_LVMH____1811956381__AdWords,0)+
  ifnull ( All_Conv__Value__Transactions_GR__1_1__Main_Profile_SephoraEUR_SR_GRC_EMEA_Social_YouTube_PML_EUR_LVMH____2050118767__AdWords,0)+
  ifnull ( All_Conv__Value__Transactions_IT__1_3__All_environments_SephoraEUR_SR_ITA_EMEA_Search_Google_LAB_EUR_LVMH____4091444686__AdWor,0)+
 ifnull ( All_Conv__Value__Transactions_PL__1_1__Main_Profile_SephoraEUR_SR_POL_EMEA_Search_Google_LAB_EUR_LVMH_Ongoing___5483090326__Ad,0)+
 ifnull ( All_Conv__Value__Transactions_PL__1_1__Main_Profile_SephoraEUR_SR_POL_EMEA_Search_Google_PML_EUR_LVMH_CSS___1896445829__AdWord,0)+
 ifnull ( All_Conv__Value__Transactions_PT__1_1__Main_Profile_SephoraEUR_SR_PRT_EMEA_Search_Google_LAB_EUR_LVMH____5663836856__AdWords,0)+
 ifnull ( All_Conv__Value__Transactions_SA__1_2__Main_Profile___OLD_SephoraEUR_SR_SAU_EMEA_Search_Google_PML_USD_LVMH____6826724117__AdW,0)+
  ifnull (All_Conv__Value__Transactions_SE__1_1__Main_Profile_SephoraEUR_SR_SWE_EMEA_Search_Google_LAB_EUR_LVMH____7164460246__AdWords,0)+
  ifnull (All_Conv__Value__Transactions_UAE__1_6__United_Arab_Emirates_SephoraEUR_SR_UAE_EMEA_Search_Google_PML_USD_LVMH____1801457948__,0)+
  ifnull (All_Conv__Value__Transactions_Google_Ads_SephoraEUR_SR_BEL_EMEA_Search_Google_LAB_EUR_LVMH____9439832600__AdWords,0)+
  ifnull (All_Conv__Value__Transactions_Google_Ads_SephoraEUR_SR_FRA_EMEA_Search_Google_LAB_EUR_LVMH_CSS___4746983045__AdWords,0)+
  ifnull (All_Conv__Value__UA_Transactions_AE__1_1__Main_Profile___NEW_SephoraEUR_SR_UAE_EMEA_Search_Google_PML_USD_LVMH____1801457948__,0)+
  ifnull (All_Conv__Value__UA_Transactions_SA__1_1__Main_Profile___NEW_SephoraEUR_SR_UAE_EMEA_Search_Google_PML_USD_LVMH____1801457948__,0)+
  ifnull (Conv__Value__Sale_SephoraEUR_SR_ROU_EMEA_Search_Google_LAB_EUR_LVMH____7778606678__AdWords,0)) as revenue
from {{ref('stg_funnel_global_data')}} 
where Data_Source_type = 'adwords'
group by 1,2,3,4,5,6

union all 

select  
      Date , 
      Country_, 
      Data_Source_type as regie_source, 
      Data_Source_name as source_name,
      campaign,
      media_type, 
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost, 
      sum(Purchases_Conversion_Value__Facebook_Ads) as revenue
from {{ref('stg_funnel_global_data')}} 
where Data_Source_type = 'facebookads'
group by 1,2,3,4,5,6

union all 

select  
      Date , 
      Country_, 
      Data_Source_type as regie_source, 
      Data_Source_name as source_name,
      campaign,
      media_type, 
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost , 
      sum(Approved_transaction_amount__Awin) as revenue
from {{ref('stg_funnel_global_data')}} 
where Data_Source_type = 'awin'
group by 1,2,3,4,5,6

union all 

select  
      Date , 
      Country_, 
      Data_Source_type as regie_source, 
      Data_Source_name as source_name,
      campaign,
      media_type, 
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost,
      sum(Order_Value__Criteo) as revenue
from {{ref('stg_funnel_global_data')}} 
where Data_Source_type = 'criteo'
group by 1,2,3,4,5,6

union all 

select  
      Date , 
      Country_, 
      Data_Source_type as regie_source, 
      Data_Source_name as source_name,
      campaign,
      media_type, 
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost,
      sum(Purchases_Value_Web___Snapchat) as revenue
from {{ref('stg_funnel_global_data')}} 
where Data_Source_type = 'snapchat'
group by 1,2,3,4,5,6

union all 

select  
      Date , 
      Country_, 
      Data_Source_type as regie_source, 
      Data_Source_name as source_name,
      campaign,
      media_type, 
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost, 
      sum(PC_Conversion_Value__RTB_House) as revenue
from {{ref('stg_funnel_global_data')}} 
where Data_Source_type = 'rtbhouse'
group by 1,2,3,4,5,6

union all 

 select  
      Date , 
      Country_, 
      Data_Source_type as regie_source, 
      Data_Source_name as source_name,
      campaign,
      media_type, 
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost , 
      sum(Order_value_Checkout__Pinterest) as revenue
from {{ref('stg_funnel_global_data')}}
where Data_Source_type = 'pinterest'
group by 1,2,3,4,5,6

union all 

select  
      Date , 
      Country_, 
      Data_Source_type as regie_source, 
      Data_Source_name as source_name,
      campaign,
      media_type, 
      sum(impressions) as impressions, 
      sum(clicks) as clicks, 
      sum(cost) as cost,
      sum(Total_Complete_Payment_Value__TikTok) as revenue
from {{ref('stg_funnel_global_data')}} 
where Data_Source_type = 'tiktok'
group by 1,2,3,4,5,6

order by impressions desc
