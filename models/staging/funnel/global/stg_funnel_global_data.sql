{{
  config(
    materialized = 'table',
    labels = {'type': 'funnel_data', 'contains_pie': 'no', 'category':'staging'}  
  )
}}

with date_range as (
select
    '20200101' as start_date,
    format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as end_date 
    ) , 

data_agg as ( 
select 
  distinct
    date,
    lower(concat(Data_Source_type,Data_Source_name)) as info_source_id,
    data_source_type,
    data_source_id,
    currency,
    cost,
    impressions,
    clicks,
    campaign,
    media_type,
    data_source,
    data_source_name,
    funnel_account_name,
    day_of_week,
    month,
    month_number,
    week,
    week_number,
    week_number_iso,
    year,
    ad__adwords,
    ad_account_customer_id__adwords,
    ad_account_name__adwords,
    ad_account_timezone__adwords,
    ad_group_id__adwords,
    ad_group_name__adwords,
    ad_group_type__adwords,
    ad_id__adwords,
    ad_name__adwords,
    ad_type__adwords,
    campaign__adwords,
    campaign_id__adwords,
    campaign_type__adwords,
    clicks__adwords,
    cost__adwords,
    engagements__adwords,
    impressions__adwords,
    interactions__adwords,
    video_views__adwords,
    views__adwords,
    advertiser_country__awin,
    advertiser_id__awin,
    advertiser_name__awin,
    publisher_name__awin,
    publisher_id__awin,
    confirmed_amount__awin,
    total_amount__awin,
    clicks__awin,
    impressions__awin,
    total_commission__awin,
    approved_transaction_amount__awin,
    approved_transaction_commission__awin,
    transaction_commission__awin,
    transaction_amount__awin,
    clicks__criteo,
    cost__criteo,
    impressions__criteo,
    order_value__criteo,
    sales__criteo,
    sales_post_click_1d__criteo,
    sales_post_click_7d__criteo,
    ad_set__criteo,
    ad_set_id__criteo,
    advertiser__criteo,
    advertiser_id__criteo,
    category_id__criteo,
    category_name__criteo,
    device__criteo,
    ad_account_id__facebook_ads,
    ad_id__facebook_ads,
    ad_name__facebook_ads,
    ad_set_id__facebook_ads,
    ad_set_name__facebook_ads,
    campaign_id__facebook_ads,
    campaign_name__facebook_ads,
    campaign_objective__facebook_ads,
    clicks_all__facebook_ads,
    amount_spent__facebook_ads,
    impressions__facebook_ads,
    spend__snapchat,
    shares__snapchat,
    saves__snapchat,
    leads__snapchat,
    impressions__snapchat,
    story_opens__snapchat,
    story_completes__snapchat,
    swipes__snapchat,
    video_views__snapchat,
    account_id__snapchat,
    ad_account_name__snapchat,
    ad_id__snapchat,
    ad_name__snapchat,
    ad_type__snapchat,
    campaign_id__snapchat,
    campaign_name__snapchat,
    squad_id__snapchat,
    squad_name__snapchat,
    likes__tiktok,
    paid_comments__tiktok,
    paid_follows__tiktok,
    campaign_cost__rtb_house,
    campaign_id__rtb_house,
    device_type__rtb_house,
    sub_campaign__rtb_house,
    impressions__rtb_house,
    attr_pc_conversion_value__rtb_house,
    pc_conversion_count__rtb_house,
    pc_conversion_value__rtb_house,
    clicks__rtb_house,
    pv_conversion_count__rtb_house,
    pv_conversion_value__rtb_house,
    advertiser_name__pinterest,
    advertiser_id__pinterest,
    campaign_name__pinterest,
    campaign_status__pinterest,
    campaign_id__pinterest,
    conversions__pinterest,
    spend__pinterest,
    total_web_checkout_value__pinterest,
    total_web_checkout__pinterest,
    total_conversion_value__pinterest,
    total_conversion_quantity__pinterest,
    order_value_checkout__pinterest,
    order_quantity_checkout__pinterest,
    reach__pinterest,
    purchases__facebook_ads,
    website_purchases__facebook_ads,
    purchases_conversion_value__facebook_ads,
    website_purchases_conversion_value__facebook_ads,
    number_of_transactions__awin,
    cost__rtb_house,
    purchases_value_total___snapchat,
    purchases_total___snapchat,
    purchases_value_web___snapchat,
    purchases_web___snapchat,
    total_purchase_skan__tiktok,
    transactions_ae__1_1__main_profile___new_sephoraeur_sr_uae_emea_search_google_pml_usd_lvmh____1801457948__adwords,
    transactions_ae__1_2__main_profile___old_sephoraeur_sr_uae_emea_search_google_pml_usd_lvmh____1801457948__adwords,
    transactions_all_web_site_data_sephoraeur_sr_rus_emea_search_google_pml_rub_lvmh_brand___9398486580__adwords,
    transactions_all_web_site_data_sephoraeur_sr_rus_emea_search_google_pml_rub_lvmh_general___7358146252__adwords,
    transactions_ch__1_1__main_profile_eur_sephoraeur_sr_ch_emea_search_google_lab_eur_lvmh____6901394591__adwords,
    transactions_cz__1_1__main_profile_sephora___9953053214__adwords,
    transactions_de__1_1__main_profile_sephoraeur_sr_deu_emea_search_google_lab_eur_lvmh____8382445893__adwords,
    transactions_dk__1_1__main_profile_sephoraeur_sr_dnk_emea_search_google_lab_eur_lvmh____1114321413__adwords,
    transactions_es__1_1__main_profile_sephoraeur_sr_esp_emea_search_google_lab_eur_lvmh____4444976669__adwords,
    transactions_es__1_1__main_profile_sephoraeur_sr_esp_emea_search_google_lab_eur_lvmh_css___7683939009__adwords,
    transactions_es__1_1__main_profile_sephoraeur_sr_esp_emea_search_google_lab_eur_lvmh_youtube___8018945072__adwords,
    transactions_es___gtag_sephoraeur_sr_esp_emea_search_google_lab_eur_lvmh_youtube___8018945072__adwords,
    transactions_fr__1_1__main_profile_sephoraeur_sr_bel_emea_search_google_lab_eur_lvmh____9439832600__adwords,
    transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh____7438136108__adwords,
    transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_css___4746983045__adwords,
    transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_css___4746983045__adwords_1,
    transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_trade_marketing___9025602533__adwords,
    transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_visibility___6943817893__adwords,
    transactions_gr__1_1__main_profile_sephoraeur_sr_grc_emea_search_google_pml_eur_lvmh____1811956381__adwords,
    transactions_gr__1_1__main_profile_sephoraeur_sr_grc_emea_social_youtube_pml_eur_lvmh____2050118767__adwords,
    transactions_it__1_3__all_environments_sephoraeur_sr_ita_emea_search_google_lab_eur_lvmh____4091444686__adwords,
    transactions_pl__1_1__main_profile_sephoraeur_sr_pol_emea_search_google_lab_eur_lvmh_ongoing___5483090326__adwords,
    transactions_pl__1_1__main_profile_sephoraeur_sr_pol_emea_search_google_pml_eur_lvmh_css___1896445829__adwords,
    transactions_pt__1_1__main_profile_sephoraeur_sr_prt_emea_search_google_lab_eur_lvmh____5663836856__adwords,
    transactions_sa__1_2__main_profile___old_sephoraeur_sr_sau_emea_search_google_pml_usd_lvmh____6826724117__adwords,
    transactions_se__1_1__main_profile_sephoraeur_sr_swe_emea_search_google_lab_eur_lvmh____7164460246__adwords,
    transactions_google_ads_sephoraeur_sr_bel_emea_search_google_lab_eur_lvmh____9439832600__adwords,
    transactions_uae__1_6__united_arab_emirates_sephoraeur_sr_uae_emea_search_google_pml_usd_lvmh____1801457948__adwords,
    transactions_google_ads_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_css___4746983045__adwords,
    all_conv__value__ga_transaction_sephoraeur_sr_tur_emea_search_google_pml_try_lvmh____8073571566__adwords,
    all_conv__value__sephoraeur_site_sales_purchaseconfirmation_sales_transaction_20192202_site_sale_sephoraeur_sr_tur_emea_search,
    all_conv__value__transactions_ae__1_1__main_profile___new_sephoraeur_sr_uae_emea_search_google_pml_usd_lvmh____1801457948__adw,
    all_conv__value__transactions_ae__1_2__main_profile___old_sephoraeur_sr_uae_emea_search_google_pml_usd_lvmh____1801457948__adw,
    all_conv__value__transactions_all_web_site_data_sephoraeur_sr_rus_emea_search_google_pml_rub_lvmh_brand___9398486580__adwords,
    all_conv__value__transactions_ch__1_1__main_profile_eur_sephoraeur_sr_ch_emea_search_google_lab_eur_lvmh____6901394591__adword,
    all_conv__value__transactions_all_web_site_data_sephoraeur_sr_rus_emea_search_google_pml_rub_lvmh_general___7358146252__adword,
    all_conv__value__transactions_cz__1_1__main_profile_sephora___9953053214__adwords,
    all_conv__value__transactions_de__1_1__main_profile_sephoraeur_sr_deu_emea_search_google_lab_eur_lvmh____8382445893__adwords,
    all_conv__value__transactions_dk__1_1__main_profile_sephoraeur_sr_dnk_emea_search_google_lab_eur_lvmh____1114321413__adwords,
    all_conv__value__transactions_es__1_1__main_profile_sephoraeur_sr_esp_emea_search_google_lab_eur_lvmh____4444976669__adwords,
    all_conv__value__transactions_es__1_1__main_profile_sephoraeur_sr_esp_emea_search_google_lab_eur_lvmh_css___7683939009__adword,
    all_conv__value__transactions_es__1_1__main_profile_sephoraeur_sr_esp_emea_search_google_lab_eur_lvmh_youtube___8018945072__ad,
    all_conv__value__transactions_es___gtag_sephoraeur_sr_esp_emea_search_google_lab_eur_lvmh_youtube___8018945072__adwords,
    all_conv__value__transactions_fr__1_1__main_profile_sephoraeur_sr_bel_emea_search_google_lab_eur_lvmh____9439832600__adwords,
    all_conv__value__transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh____7438136108__adwords,
    all_conv__value__transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_css___4746983045__adword,
    all_conv__value__transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_css___4746983045__adword_1,
    all_conv__value__transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_trade_marketing___902560,
    all_conv__value__transactions_fr__1_1__main_profile_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_visibility___6943817893_,
    all_conv__value__transactions_gr__1_1__main_profile_sephoraeur_sr_grc_emea_search_google_pml_eur_lvmh____1811956381__adwords,
    all_conv__value__transactions_gr__1_1__main_profile_sephoraeur_sr_grc_emea_social_youtube_pml_eur_lvmh____2050118767__adwords,
    all_conv__value__transactions_it__1_3__all_environments_sephoraeur_sr_ita_emea_search_google_lab_eur_lvmh____4091444686__adwor,
    all_conv__value__transactions_pl__1_1__main_profile_sephoraeur_sr_pol_emea_search_google_lab_eur_lvmh_ongoing___5483090326__ad,
    all_conv__value__transactions_pl__1_1__main_profile_sephoraeur_sr_pol_emea_search_google_pml_eur_lvmh_css___1896445829__adword,
    all_conv__value__transactions_pt__1_1__main_profile_sephoraeur_sr_prt_emea_search_google_lab_eur_lvmh____5663836856__adwords,
    all_conv__value__transactions_sa__1_2__main_profile___old_sephoraeur_sr_sau_emea_search_google_pml_usd_lvmh____6826724117__adw,
    all_conv__value__transactions_se__1_1__main_profile_sephoraeur_sr_swe_emea_search_google_lab_eur_lvmh____7164460246__adwords,
    all_conv__value__transactions_uae__1_6__united_arab_emirates_sephoraeur_sr_uae_emea_search_google_pml_usd_lvmh____1801457948__,
    all_conv__value__transactions_google_ads_sephoraeur_sr_bel_emea_search_google_lab_eur_lvmh____9439832600__adwords,
    all_conv__value__transactions_google_ads_sephoraeur_sr_fra_emea_search_google_lab_eur_lvmh_css___4746983045__adwords,
    all_conv__value__ua_transactions_ae__1_1__main_profile___new_sephoraeur_sr_uae_emea_search_google_pml_usd_lvmh____1801457948__,
    all_conv__value__ua_transactions_sa__1_1__main_profile___new_sephoraeur_sr_uae_emea_search_google_pml_usd_lvmh____1801457948__,
    ad_id__tiktok,
    ad_name__tiktok,
    adgroup_id__tiktok,
    adgroup_name__tiktok,
    advertiser_id__tiktok,
    advertiser_name__tiktok,
    campaign_name__tiktok,
    campaign_id__tiktok,
    conversions__tiktok,
    clicks__tiktok,
    impressions__tiktok,
    total_cost__tiktok,
    total_complete_payment_value__tiktok,
    total_complete_payment__tiktok,
    sale_sephoraeur_sr_rou_emea_search_google_lab_eur_lvmh____7778606678__adwords,
    conv__value__sale_sephoraeur_sr_rou_emea_search_google_lab_eur_lvmh____7778606678__adwords
from {{ source('funnel', 'funnel_data_*') }}  ,
date_range
 where _table_suffix between start_date and end_date
), 

data_orga as ( 

  select lower(source_id) as source_id , 
         DATA_SOURCE_TYPE_NAME, 
         NAME, 
         Country_
   from  {{ source('funnel', 'funnel_account_orga') }}
)

select * from data_agg 
left join data_orga 
on data_agg.info_source_id = data_orga.source_id


