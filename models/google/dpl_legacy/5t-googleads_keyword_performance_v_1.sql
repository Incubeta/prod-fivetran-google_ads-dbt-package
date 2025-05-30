
{{
  config(
    alias= '5t-googleads-keyword_performance-v1'
  )
}}

with exchange_source AS (
    select 
        day,
        currency_code,
        ex_rate
    from {{ref('openexchange_rates','stg_openexchange_rates__openexchange_report_v1')}}
)




SELECT
SAFE_CAST( source_a.date AS DATE ) day,
SAFE_CAST( absolute_top_impression_percentage AS FLOAT64 ) absolute_top_impression,
SAFE_CAST( NULL AS FLOAT64 ) active_view_average_cpm,
SAFE_CAST( NULL AS INT64 ) active_view_measurability,
SAFE_CAST( NULL AS INT64 ) active_view_measurable_impressions,
SAFE_CAST( NULL AS FLOAT64 ) active_view_viewability,
SAFE_CAST( NULL AS FLOAT64 ) active_view_viewable_ctr,
SAFE_CAST( NULL AS INT64 ) active_view_viewable_impressions,
SAFE_CAST( ad_final_url_suffix AS STRING ) ad_final_url_suffix,
SAFE_CAST( ad_group_name AS STRING ) ad_group,
SAFE_CAST( ad_group_id AS STRING ) ad_group_id,
SAFE_CAST( ad_group_status AS STRING ) ad_group_state,
SAFE_CAST( campaign_ad_serving_optimization_status AS STRING ) ad_serving_optimization_status,
SAFE_CAST( all_conversions AS FLOAT64 ) all_conversions,
SAFE_CAST( ad_group_base_ad_group AS STRING ) base_ad_group_id,
SAFE_CAST( campaign_base_campaign AS STRING ) base_campaign_id,
SAFE_CAST( campaign_bidding_strategy_type AS STRING ) bid_strategy_type_campaign,
SAFE_CAST( campaign_name AS STRING ) campaign_name,
SAFE_CAST( campaign_id AS STRING ) campaign_id,
SAFE_CAST( campaign_serving_status AS STRING ) campaign_serving_status,
SAFE_CAST( campaign_status AS STRING ) campaign_state,
SAFE_CAST( clicks AS INT64 ) clicks,
SAFE_CAST( NULL AS FLOAT64 ) conversion_value_current_model,
SAFE_CAST( conversions AS FLOAT64 ) conversions,
SAFE_DIVIDE( SAFE_CAST( cost_micros AS FLOAT64 ), 1000000 ) cost,
SAFE_CAST( customer_currency_code AS STRING ) currency,
SAFE_CAST( customer_id AS STRING ) customer_id,
SAFE_CAST( customer_descriptive_name AS STRING ) customer_name,
SAFE_CAST( NULL AS FLOAT64 ) default_max_cpc,
SAFE_CAST( device AS STRING ) device,
SAFE_CAST( engagements AS INT64 ) engagements,
SAFE_CAST( percent_cpc_enhanced_cpc_enabled AS BOOLEAN ) enhanced_cpc_enabled,
SAFE_CAST( ad_final_urls AS STRING ) final_url,
SAFE_CAST( ad_final_app_urls AS STRING ) final_app_urls,
SAFE_CAST( impressions AS INT64 ) impressions,
SAFE_CAST( interaction_event_types AS STRING ) interaction_types,
SAFE_CAST( interactions AS INT64 ) interactions,
SAFE_CAST( info_text AS STRING ) keyword,
SAFE_CAST( NULL AS STRING ) keyword_id,
SAFE_CAST( NULL AS STRING ) mcc_id,
SAFE_CAST( NULL AS STRING ) mcc_name,
SAFE_CAST( NULL AS FLOAT64 ) maximum_cpm,
SAFE_CAST( ad_final_mobile_urls AS STRING ) mobile_final_url,
SAFE_CAST( ad_network_type AS STRING ) network,
SAFE_CAST( customer_time_zone AS STRING ) time_zone,
SAFE_CAST( top_impression_percentage AS FLOAT64 ) top_impression,
SAFE_CAST( value_per_conversion AS FLOAT64 ) value_per_conversion,
SAFE_CAST( video_quartile_p_100_rate AS FLOAT64 ) video_played_to_100,
SAFE_CAST( video_quartile_p_25_rate AS FLOAT64 ) video_played_to_25,
SAFE_CAST( video_quartile_p_50_rate AS FLOAT64 ) video_played_to_50,
SAFE_CAST( video_quartile_p_75_rate AS FLOAT64 ) video_played_to_75,
SAFE_CAST( video_view_rate AS FLOAT64 ) view_rate,
SAFE_CAST( view_through_conversions AS STRING ) view_through_conversions,
SAFE_CAST( video_views AS INT64 ) views,
SAFE_CAST( all_conversions_value AS FLOAT64 ) total_conversion_value,
'googleads_keyword_performance_v_1' AS raw_origin,
(SAFE_DIVIDE( SAFE_CAST( cost_micros AS FLOAT64 ), 1000000 ) / exchange_source.ex_rate) _gbp_cost,
(SAFE_CAST(all_conversions_value AS FLOAT64) / exchange_source.ex_rate) _gbp_revenue,

/* Below macro creates additional fields based on form inputs for "Subaccounts, campaign delimitter, custom fields" */
{{add_fields("campaign_name")}} /* Replace with the report's campaign name field */

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM
/* source raw main table is referred as source_a and 
openexchange table is being referred as exchange_source */
  {{ source('google', 'googleads_keyword_performance_v_1') }} source_a

LEFT JOIN exchange_source

ON

SAFE_CAST(source_a.date AS DATE) = exchange_source.day
/* Jinja var if default field has null value.Replace the default field based on the report */
AND LOWER(IFNULL(TRIM(customer_currency_code),'{{var('account_currency')}}')) = exchange_source.currency_code
/*When the currency value doesn't exist in the default report we add
account currency variable by default */


