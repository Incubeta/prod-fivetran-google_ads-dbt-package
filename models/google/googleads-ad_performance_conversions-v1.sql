{{
  custom_config(
    alias=var('googleads_ad_performance_conversions_v1_alias','googleads-ad_performance_conversions-v1'),
    field="Day")
}}

SELECT
    SAFE_CAST(ad_id AS STRING) Ad_ID,
    SAFE_CAST(ad_group AS STRING) Ad_group,
    SAFE_CAST(ad_group_id AS STRING) Ad_group_ID,
    SAFE_CAST(Ad_name AS STRING) ad_name,
    SAFE_CAST(campaign_advertising_channel_type AS STRING) Advertising_channel,
    SAFE_CAST(all_conversions_value_by_conversion_date AS STRING) All_conversion_value__by_conversion_time_,
    SAFE_CAST(all_conversions AS STRING) All_conversions,
    SAFE_CAST(all_conversions_by_conversion_date AS STRING) All_conversions__by_conversion_time_,
    SAFE_CAST(all_conversions_value AS STRING) All_conversions_value,
    SAFE_CAST(campaign_name AS STRING) Campaign,
    SAFE_CAST(campaign_id AS STRING) Campaign_ID,
    SAFE_CAST(conversion_tracking_setting_conversion_tracking_id AS STRING) Conversion_Tracker_ID,
    SAFE_CAST(conversion_action_name AS STRING) Conversion_name,
    SAFE_CAST(conversions_value_by_conversion_date AS STRING) Conversion_value__by_conversion_time_,
    SAFE_CAST(conversions AS STRING) Conversions,
    SAFE_CAST(conversions_by_conversion_date AS STRING) Conversions__by_conversion_time_,
    SAFE_CAST(cross_device_conversions AS STRING) Cross_device_conversions,
    SAFE_CAST(customer_id AS STRING) Customer_ID,
    SAFE_CAST(customer_descriptive_name AS STRING) Customer_name,
    SAFE_CAST(date AS DATE) Day,
    SAFE_CAST(device AS STRING) Device,
    SAFE_CAST(conversions_value AS STRING) Total_conversion_value,
    SAFE_CAST(view_through_conversions AS STRING) View_through_conversions,
    SAFE_CAST(TRIM(customer_currency_code) AS STRING) Currency,

FROM {{ source('google', 'googleads_ad_performance_conversions_v_1') }}
