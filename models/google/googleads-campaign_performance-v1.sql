{{
  custom_config(
    alias=var('googleads_campaign_performance_v1_alias','googleads-campaign_performance-v1'),
    field="Day")
}}

SELECT
    SAFE_CAST( date AS DATE ) Day,
    SAFE_CAST( customer_id AS STRING ) Customer_ID,
    SAFE_CAST( customer_descriptive_name AS STRING ) Customer_name,
    SAFE_CAST( id AS STRING ) Campaign_ID,
    SAFE_CAST( name AS STRING )	Campaign,
    SAFE_CAST( TRIM(customer_currency_code) AS STRING ) Currency,
    SAFE_CAST( advertising_channel_sub_type AS STRING ) Advertising_sub_channel,
    SAFE_CAST( advertising_channel_type AS STRING ) Advertising_channel,
    SAFE_CAST( base_campaign AS STRING ) Base_campaign_ID,
    SAFE_CAST( bidding_strategy AS STRING ) Bid_strategy_ID,
    SAFE_CAST( NULL AS STRING ) Bid_strategy_name,
    SAFE_CAST( bidding_strategy_type AS STRING ) Bid_strategy_type,
    SAFE_CAST( bidding_strategy_type AS STRING ) Bid_strategy_type__campaign_,
    SAFE_CAST( campaign_budget AS STRING ) Budget_ID,
    SAFE_CAST( serving_status AS STRING ) Campaign_serving_status,
    SAFE_CAST( status AS STRING ) Campaign_state,
    SAFE_CAST( device AS STRING	) Device,
    SAFE_CAST( end_date AS STRING ) End_date,
    SAFE_CAST( final_url_suffix AS STRING ) Final_URL_suffix,
    SAFE_CAST( interaction_event_types AS STRING) Interaction_types,
    SAFE_CAST( ad_network_type AS STRING ) Network,
    SAFE_CAST( NULL AS STRING ) MCC_ID,
    SAFE_CAST( NULL AS STRING ) MCC_name,
    SAFE_CAST( start_date AS STRING ) Start_date,
    SAFE_CAST( customer_time_zone AS STRING ) Time_zone,
    SAFE_CAST( absolute_top_impression_percentage AS STRING ) Absolute_Top_Impression____,
    SAFE_CAST( active_view_measurability AS STRING ) Active_view_measurability,
    SAFE_CAST( SAFE_DIVIDE( active_view_measurable_cost_micros, 1000000 ) AS STRING ) Active_view_measurable_cost,
    SAFE_CAST( active_view_measurable_impressions AS STRING ) Active_view_measurable_impressions,
    SAFE_CAST( active_view_viewability AS STRING ) Active_view_viewability,
    SAFE_CAST( all_conversions_value_by_conversion_date AS STRING ) All_conversion_value__by_conversion_time_,
    SAFE_CAST( all_conversions_by_conversion_date AS STRING ) All_conversions__by_conversion_time_,
    SAFE_CAST( search_click_share AS STRING ) Click_share,
    SAFE_CAST( clicks AS STRING	) Clicks,
    SAFE_CAST( conversions AS STRING ) Conversions,
    SAFE_CAST( all_conversions AS STRING )	All_conversions,
    SAFE_CAST( all_conversions_value AS	STRING ) All_conversions_value,
    SAFE_CAST( conversions_value_by_conversion_date AS STRING ) Conversion_value__by_conversion_time_,
    SAFE_CAST( conversions_by_conversion_date AS STRING ) Conversions__by_conversion_time_,
    SAFE_CAST( SAFE_DIVIDE( cost_micros, 1000000 ) AS STRING ) Cost,
    SAFE_CAST( engagements AS STRING) Engagements,
    SAFE_CAST( manual_cpc_enhanced_cpc_enabled AS STRING ) Enhanced_CPC_enabled,
    SAFE_CAST( impressions AS STRING ) Impressions,
    SAFE_CAST( interactions AS STRING ) Interactions,
    SAFE_CAST( search_absolute_top_impression_share AS STRING ) Search_absolute_top_impression_share,
    SAFE_CAST( search_exact_match_impression_share AS STRING ) Search_exact_match_impression_share,
    SAFE_CAST( search_impression_share AS STRING ) Search_impression_share,
    SAFE_CAST( search_top_impression_share AS STRING ) Search_top_impression_share,
    SAFE_CAST( top_impression_percentage AS STRING ) Top_Impression____,
    SAFE_CAST( SAFE_DIVIDE( campaign_budget_total_amount_micros, 1000000 ) AS STRING ) Total_budget_amount,
    SAFE_CAST( value_per_conversion	AS STRING ) Value_per_conversion,
    SAFE_CAST( video_quartile_p_100_rate AS STRING ) Video_played_to_100_,
    SAFE_CAST( video_quartile_p_25_rate AS STRING )	Video_played_to_25_,
    SAFE_CAST( video_quartile_p_50_rate AS STRING )	Video_played_to_50_,
    SAFE_CAST( video_quartile_p_75_rate AS STRING )	Video_played_to_75_,
    SAFE_CAST( view_through_conversions AS STRING ) View_through_conversions,
    SAFE_CAST( video_views AS STRING ) Views,
    SAFE_CAST( conversions_value AS STRING ) Total_conversion_value,

FROM {{ source('google', 'googleads_campaign_performance_v_1') }}
