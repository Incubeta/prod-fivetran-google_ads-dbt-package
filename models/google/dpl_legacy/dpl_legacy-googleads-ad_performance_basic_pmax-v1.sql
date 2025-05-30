
{{
  config(
    alias= 'googleads-ad_performance_basic_pmax-v1'
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
SAFE_CAST( source_a.Day AS DATE ) day, 
SAFE_CAST( Absolute_Top_Impression____ AS INT64	) absolute_top_impression,
SAFE_CAST( Active_view_average_CPM AS FLOAT64 )	active_view_average_cpm,
SAFE_CAST( active_view_measurability AS FLOAT64 ) active_view_measurability,
SAFE_CAST( Active_view_measurable_cost AS FLOAT64 ) active_view_measurable_cost,
SAFE_CAST( active_view_measurable_impressions AS INT64 ) active_view_measurable_impressions,
SAFE_CAST( active_view_viewability AS FLOAT64 ) active_view_viewability,
SAFE_CAST( Advertising_channel AS STRING ) advertising_channel,
SAFE_CAST( Advertising_sub_channel AS STRING ) advertising_sub_channel,
SAFE_CAST( All_conversion_value__by_conversion_time_ AS	FLOAT64	) all_conversion_value_by_conversion_time,
SAFE_CAST( All_conversions AS FLOAT64 )	all_conversions,
SAFE_CAST( All_conversions__by_conversion_time_	AS FLOAT64 ) all_conversions_by_conversion_time,
SAFE_CAST( All_conversions_value AS	FLOAT64	) all_conversions_value,
SAFE_CAST( Base_campaign_ID AS STRING ) base_campaign_id,
SAFE_CAST( Bid_strategy_type__campaign_ AS STRING ) bid_strategy_type_campaign,
SAFE_CAST( Budget_ID AS STRING ) budget_id,
SAFE_CAST( Campaign_ID AS STRING ) campaign_id,
SAFE_CAST( Campaign_Bid_strategy_ID AS STRING ) campaign_bid_strategy_id,
SAFE_CAST( Campaign AS STRING )	campaign_name,
SAFE_CAST( Campaign_serving_status AS STRING ) campaign_serving_status,
SAFE_CAST( Campaign_state AS STRING	) campaign_state,
SAFE_CAST( clicks AS INT64 ) clicks,
SAFE_CAST( Conversion_value__by_conversion_time_ AS	FLOAT64	) conversion_value_by_conversion_time,
SAFE_CAST( Conversions AS FLOAT64 )	conversions,
SAFE_CAST( Conversions__by_conversion_time_	AS FLOAT64 ) conversions_by_conversion_time,
SAFE_CAST( Cost	AS FLOAT64 ) cost,
SAFE_CAST( TRIM(Currency) AS STRING ) currency,
SAFE_CAST( Customer_ID AS STRING ) customer_id,
SAFE_CAST( Customer_name AS STRING ) customer_name,
SAFE_CAST( Device AS STRING	) device,
SAFE_CAST( End_date AS DATE ) end_date,
SAFE_CAST( engagements AS INT64 ) engagements,
SAFE_CAST( Enhanced_CPC_enabled	AS STRING )	enhanced_cpc_enabled,
SAFE_CAST( Final_URL_suffix AS STRING )	final_url_suffix,
SAFE_CAST( impressions AS INT64	) impressions,
SAFE_CAST( Interaction_types AS	STRING ) interaction_types,
SAFE_CAST( interactions AS INT64 ) interactions,
SAFE_CAST( MCC_ID AS STRING	) mcc_id,
SAFE_CAST( MCC_name AS STRING )	mcc_name,
SAFE_CAST( Network AS STRING ) network,
SAFE_CAST( Start_date AS DATE ) start_date,
SAFE_CAST( time_zone AS	STRING ) time_zone,
SAFE_CAST( Top_Impression____ AS INT64 ) top_impression,
SAFE_CAST( Value_per_conversion	AS FLOAT64 ) value_per_conversion,
SAFE_CAST( video_played_to_100_	AS FLOAT64 ) video_played_to_100,
SAFE_CAST( video_played_to_25_ AS FLOAT64 ) video_played_to_25,
SAFE_CAST( video_played_to_50_ AS FLOAT64 ) video_played_to_50,
SAFE_CAST( video_played_to_75_ AS FLOAT64 ) video_played_to_75,
SAFE_CAST( View_through_conversions AS INT64 ) view_through_conversions,
SAFE_CAST( views AS	INT64 )	views,
SAFE_CAST( Total_conversion_value AS FLOAT64 ) total_conversion_value,
'googleads-ad_performance_basic_pmax-v1' AS raw_origin,
(SAFE_CAST( Cost AS FLOAT64) / exchange_source.ex_rate ) _gbp_cost,
(SAFE_CAST( Total_conversion_value AS FLOAT64) / exchange_source.ex_rate ) _gbp_revenue, --value or cost???

/* Below macro creates additional fields based on form inputs for "Subaccounts, campaign delimitter, custom fields" */
{{add_fields("campaign")}} /* Replace with the report's campaign name field */

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM
/* source raw main table is referred as source_a and 
openexchange table is being referred as exchange_source */
  {{ source('raw_main','googleads-ad_performance_basic_pmax_v_1') }} source_a

LEFT JOIN exchange_source

ON

SAFE_CAST(source_a.day AS DATE) = exchange_source.day 
/* Jinja var if default field has null value..Replace the default field based on the report */
AND LOWER(IFNULL(TRIM(Currency),'{{var('account_currency')}}')) = exchange_source.currency_code 
/*When the currency value doesnt exist in the default report we add 
account currency variable by default */


