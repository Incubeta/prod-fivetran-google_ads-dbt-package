with exchange_source AS (
    select 
        day,
        currency_code,
        ex_rate
    from {{ref('openexchange_rates','stg_openexchange_rates__openexchange_report_v1')}}
)
SELECT
    SAFE_CAST( source_a.Day AS DATE ) day,
    SAFE_CAST( Absolute_Top_Impression____ AS FLOAT64 ) absolute_top_impression,
    SAFE_CAST( Active_view_average_CPM AS FLOAT64 ) active_view_average_cpm,
    SAFE_CAST( Active_view_measurability AS INT64 ) active_view_measurability,
    SAFE_CAST( Active_view_measurable_impressions AS INT64 ) active_view_measurable_impressions,
    SAFE_CAST( Active_view_viewability AS FLOAT64 ) active_view_viewability,
    SAFE_CAST( Active_view_viewable_CTR AS FLOAT64 ) active_view_viewable_ctr,
    SAFE_CAST( Active_view_viewable_impressions AS INT64 ) active_view_viewable_impressions,
    SAFE_CAST( Ad_Final_URL_suffix AS STRING ) ad_final_url_suffix,
    SAFE_CAST( Ad_group AS STRING ) ad_group,
    SAFE_CAST( Ad_group_ID AS STRING ) ad_group_id,
    SAFE_CAST( Ad_group_state AS STRING ) ad_group_state,
    SAFE_CAST( Ad_serving_optimization_status AS STRING ) ad_serving_optimization_status,
    SAFE_CAST( All_conversions AS FLOAT64 ) all_conversions,
    SAFE_CAST( Base_ad_group_ID AS STRING ) base_ad_group_id,
    SAFE_CAST( Base_campaign_ID AS STRING ) base_campaign_id,
    SAFE_CAST( Bid_strategy_type__campaign_ AS STRING ) bid_strategy_type_campaign,
    SAFE_CAST( Campaign AS STRING ) campaign_name,
    SAFE_CAST( Campaign_ID AS STRING ) campaign_id,
    SAFE_CAST( Campaign_serving_status AS STRING ) campaign_serving_status,
    SAFE_CAST( Campaign_state AS STRING ) campaign_state,
    SAFE_CAST( Clicks AS INT64 ) clicks,
    SAFE_CAST( Conversion_value__current_model_ AS FLOAT64 ) conversion_value_current_model,
    SAFE_CAST( Conversions AS FLOAT64 ) conversions,
    SAFE_CAST( Cost AS FLOAT64 ) cost,
    SAFE_CAST( Currency AS STRING ) currency,
    SAFE_CAST( Customer_ID AS STRING ) customer_id,
    SAFE_CAST( Customer_name AS STRING ) customer_name,
    SAFE_CAST( Default_max__CPC AS FLOAT64 ) default_max_cpc,
    SAFE_CAST( Device AS STRING ) device,
    SAFE_CAST( Engagements AS INT64 ) engagements,
    SAFE_CAST( Enhanced_CPC_enabled AS BOOLEAN ) enhanced_cpc_enabled,
    SAFE_CAST( Final_URL AS STRING ) final_url,
    SAFE_CAST( Final_app_URLs AS STRING ) final_app_urls,
    SAFE_CAST( Impressions AS INT64 ) impressions,
    SAFE_CAST( Interaction_types AS STRING ) interaction_types,
    SAFE_CAST( Interactions AS INT64 ) interactions,
    SAFE_CAST( Keyword AS STRING ) keyword,
    SAFE_CAST( Keyword_ID AS STRING ) keyword_id,
    SAFE_CAST( MCC_ID AS STRING ) mcc_id,
    SAFE_CAST( MCC_name AS STRING ) mcc_name,
    SAFE_CAST( Maximum_CPM AS FLOAT64 ) maximum_cpm,
    SAFE_CAST( Mobile_final_URL AS STRING ) mobile_final_url,
    SAFE_CAST( Network AS STRING ) network,
    SAFE_CAST( Time_zone AS STRING ) time_zone,
    SAFE_CAST( Top_Impression____ AS FLOAT64 ) top_impression,
    SAFE_CAST( Value_per_conversion AS FLOAT64 ) value_per_conversion,
    SAFE_CAST( Video_played_to_100_ AS FLOAT64 ) video_played_to_100,
    SAFE_CAST( Video_played_to_25_ AS FLOAT64 ) video_played_to_25,
    SAFE_CAST( Video_played_to_50_ AS FLOAT64 ) video_played_to_50,
    SAFE_CAST( Video_played_to_75_ AS FLOAT64 ) video_played_to_75,
    SAFE_CAST( View_rate AS FLOAT64 ) view_rate,
    SAFE_CAST( View_through_conversions AS STRING ) view_through_conversions,
    SAFE_CAST( Views AS INT64 ) views,
    SAFE_CAST( Total_conversion_value AS FLOAT64 ) total_conversion_value,
 'googleads-keyword_performance-v1' AS raw_origin,
    (SAFE_CAST(Cost AS FLOAT64) / exchange_source.ex_rate) _gbp_cost,
    (SAFE_CAST(total_conversion_value AS FLOAT64) / exchange_source.ex_rate) _gbp_revenue, --value or cost???

/* Below macro creates additional fields based on form inputs for "Subaccounts, campaign delimitter, custom fields" */
    {{ add_fields("Campaign") }} /* Replace with the report's campaign name field */

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM
/* source raw main table is referred as source_a and
openexchange table is being referred as exchange_source */

{{ref('googleads-keyword_performance-v1')}} source_a

LEFT JOIN exchange_source

ON

    SAFE_CAST(source_a.Day AS DATE) = exchange_source.day
/* Jinja var if default field has null value..Replace the default field based on the report */
    AND LOWER(IFNULL(TRIM(currency),'{{var('account_currency')}}')) = exchange_source.currency_code
/*When the currency value doesnt exist in the default report we add
account currency variable by default */
