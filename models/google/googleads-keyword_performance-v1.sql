{{
  custom_config(
    alias=var('googleads_keyword_performance_v1_alias','googleads-keyword_performance-v1'),
    field="Day")
}}

SELECT DISTINCT
    SAFE_CAST( date AS DATE ) Day,
    SAFE_CAST( absolute_top_impression_percentage AS STRING ) Absolute_Top_Impression____,
    SAFE_CAST( NULL AS STRING ) Active_view_average_CPM,
    SAFE_CAST( NULL AS STRING ) Active_view_measurability,
    SAFE_CAST( NULL AS STRING ) Active_view_measurable_impressions,
    SAFE_CAST( NULL AS STRING ) Active_view_viewability,
    SAFE_CAST( NULL AS STRING ) Active_view_viewable_CTR,
    SAFE_CAST( NULL AS STRING ) Active_view_viewable_impressions,
    SAFE_CAST( ad_final_url_suffix AS STRING ) Ad_Final_URL_suffix,
    SAFE_CAST( ad_group_name AS STRING ) Ad_group,
    SAFE_CAST( ad_group_id AS STRING ) Ad_group_ID,
    SAFE_CAST( ad_group_status AS STRING ) Ad_group_state,
    SAFE_CAST( campaign_ad_serving_optimization_status AS STRING ) Ad_serving_optimization_status,
    SAFE_CAST( all_conversions AS STRING ) All_conversions,
    SAFE_CAST( ad_group_base_ad_group AS STRING ) Base_ad_group_ID,
    SAFE_CAST( campaign_base_campaign AS STRING ) Base_campaign_ID,
    SAFE_CAST( campaign_bidding_strategy_type AS STRING ) Bid_strategy_type__campaign_,
    SAFE_CAST( campaign_name AS STRING ) Campaign,
    SAFE_CAST( campaign_id AS STRING ) Campaign_ID,
    SAFE_CAST( campaign_serving_status AS STRING ) Campaign_serving_status,
    SAFE_CAST( campaign_status AS STRING ) Campaign_state,
    SAFE_CAST( Clicks AS STRING ) Clicks,
    SAFE_CAST( NULL AS STRING ) Conversion_value__current_model_,
    SAFE_CAST( conversions AS STRING ) Conversions,
    SAFE_CAST( SAFE_DIVIDE( cost_micros, 1000000 ) AS STRING ) Cost,
    SAFE_CAST( customer_currency_code AS STRING ) Currency,
    SAFE_CAST( customer_id AS STRING ) Customer_ID,
    SAFE_CAST( customer_descriptive_name AS STRING ) Customer_name,
    SAFE_CAST( NULL AS STRING ) Default_max__CPC,
    SAFE_CAST( device AS STRING ) Device,
    SAFE_CAST( engagements AS STRING ) Engagements,
    SAFE_CAST( percent_cpc_enhanced_cpc_enabled AS STRING ) Enhanced_CPC_enabled,
    SAFE_CAST( ad_final_urls AS STRING ) Final_URL,
    SAFE_CAST( ad_final_app_urls AS STRING ) Final_app_URLs,
    SAFE_CAST( impressions AS STRING ) Impressions,
    SAFE_CAST( interaction_event_types AS STRING ) Interaction_types,
    SAFE_CAST( interactions AS STRING ) Interactions,
    SAFE_CAST( info_text AS STRING ) Keyword,
    SAFE_CAST( NULL AS STRING ) Keyword_ID,
    SAFE_CAST( NULL AS STRING ) MCC_ID,
    SAFE_CAST( NULL AS STRING ) MCC_name,
    SAFE_CAST( NULL AS STRING ) Maximum_CPM,
    SAFE_CAST( ad_final_mobile_urls AS STRING ) Mobile_final_URL,
    SAFE_CAST( ad_network_type AS STRING ) Network,
    SAFE_CAST( customer_time_zone AS STRING ) Time_zone,
    SAFE_CAST( top_impression_percentage AS STRING ) Top_Impression____,
    SAFE_CAST( value_per_conversion AS STRING ) Value_per_conversion,
    SAFE_CAST( video_quartile_p_100_rate AS STRING ) Video_played_to_100_,
    SAFE_CAST( video_quartile_p_25_rate AS STRING )	Video_played_to_25_,
    SAFE_CAST( video_quartile_p_50_rate AS STRING )	Video_played_to_50_,
    SAFE_CAST( video_quartile_p_75_rate AS STRING )	Video_played_to_75_,
    SAFE_CAST( video_view_rate AS STRING ) View_rate,
    SAFE_CAST( view_through_conversions AS STRING ) View_through_conversions,
    SAFE_CAST( video_views AS STRING ) Views,
    SAFE_CAST( all_conversions_value AS STRING ) Total_conversion_value,
    {{ add_custom_fields() }}

 FROM {{ source('google', 'googleads_keyword_performance_v_1') }}
