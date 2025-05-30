/* Config below sets the name of the output table, this has to be
hardcoded since two different models in dbt cannot have the same destination config */

{{
  config(
    alias= 'googleads-campaign_performance-v1'
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
SAFE_CAST( source_a.Day AS DATE ) day, -- added 'a.Day'
SAFE_CAST( customer_id AS STRING ) account_id,
SAFE_CAST( customer_name AS STRING ) account_name,
SAFE_CAST( Campaign_ID AS STRING ) campaign_id	,
SAFE_CAST( Campaign AS STRING )	campaign_name	,
SAFE_CAST( TRIM(Currency)AS STRING ) currency,
SAFE_CAST( advertising_sub_channel AS STRING) advertising_sub_channel,
SAFE_CAST( advertising_channel AS STRING) advertising_channel,
SAFE_CAST( base_campaign_id AS STRING ) base_campaign_id,
SAFE_CAST( bid_strategy_id AS STRING ) bid_strategy_id,
SAFE_CAST( bid_strategy_name AS STRING ) bid_strategy_name,
SAFE_CAST( bid_strategy_type AS STRING ) bid_strategy_type,
SAFE_CAST( bid_strategy_type__campaign_  AS STRING ) bid_strategy_type_campaign,
SAFE_CAST( budget_id AS STRING ) budget_id,
SAFE_CAST( campaign_serving_status AS STRING) campaign_serving_status,
SAFE_CAST( campaign_state  AS STRING) campaign_state,
SAFE_CAST( Device AS STRING	) device,
SAFE_CAST( end_date AS DATE ) end_date,
SAFE_CAST( final_url_suffix AS STRING ) final_url_suffix,
SAFE_CAST( interaction_types   AS STRING) interaction_types,
SAFE_CAST( network AS STRING ) network,
SAFE_CAST( MCC_ID AS STRING ) mcc_id,
SAFE_CAST( MCC_name AS STRING ) mcc_name,
SAFE_CAST( start_date AS DATE )	start_date,
SAFE_CAST( time_zone AS	STRING ) time_zone,
SAFE_CAST( absolute_Top_Impression____ AS FLOAT64 ) absolute_top_impression_percentage,
SAFE_CAST( active_view_measurability   AS FLOAT64 ) active_view_measurability,
SAFE_CAST( active_view_measurable_cost AS FLOAT64 ) active_view_measurable_cost,
SAFE_CAST( active_view_measurable_impressions AS INT64 ) active_view_measurable_impressions,
SAFE_CAST( active_view_viewability AS FLOAT64 ) active_view_viewability,
SAFE_CAST( all_conversion_value__by_conversion_time_ AS FLOAT64 ) all_conversion_value_by_conversion_time,
SAFE_CAST( all_conversions__by_conversion_time_ AS FLOAT64 ) all_conversions_by_conversion_time,
SAFE_CAST( click_share AS FLOAT64 ) click_share,
SAFE_CAST( clicks AS INT64	) clicks,
SAFE_CAST( Conversions AS FLOAT64 ) conversions,
SAFE_CAST( All_conversions AS FLOAT64 )	all_conversions,
SAFE_CAST( All_conversions_value AS	FLOAT64	) all_conversions_value,
SAFE_CAST( conversion_value__by_conversion_time_ AS FLOAT64 ) conversion_value_by_conversion_time,
SAFE_CAST( conversions__by_conversion_time_ AS FLOAT64 ) conversions_by_conversion_time,
SAFE_CAST( Cost	AS FLOAT64 ) cost,
SAFE_CAST( engagements AS INT64) engagements,
SAFE_CAST( enhanced_cpc_enabled AS BOOL ) enhanced_cpc_enabled,
SAFE_CAST( impressions AS INT64 ) impressions,
SAFE_CAST( interactions AS INT64 ) interactions,
SAFE_CAST( search_absolute_top_impression_share AS FLOAT64 ) search_absolute_top_impression_share,
SAFE_CAST( search_exact_match_impression_share AS FLOAT64 )	search_exact_match_impression_share,
SAFE_CAST( search_impression_share AS FLOAT64 )	search_impression_share,
SAFE_CAST( search_top_impression_share AS FLOAT64 )	search_top_impression_share,
SAFE_CAST( top_impression____ AS FLOAT64 ) top_impression_percentage,
SAFE_CAST( total_budget_amount AS FLOAT64 )	total_budget_amount,
SAFE_CAST( value_per_conversion	AS FLOAT64 ) value_per_conversion,
SAFE_CAST( video_played_to_100_	AS FLOAT64 ) video_played_to_100,
SAFE_CAST( video_played_to_25_ AS FLOAT64 )	video_played_to_25,
SAFE_CAST( video_played_to_50_ AS FLOAT64 )	video_played_to_50,
SAFE_CAST( video_played_to_75_ AS FLOAT64 )	video_played_to_75,
SAFE_CAST( view_through_conversions AS INT64 ) view_through_conversions,
SAFE_CAST( views AS	INT64 )	views,
SAFE_CAST( Total_conversion_value AS FLOAT64 ) total_conversion_value,
'googleads-campaign_performance-v1' AS raw_origin,
(SAFE_CAST(Cost AS FLOAT64) / exchange_source.ex_rate) _gbp_cost,
(SAFE_CAST(Total_conversion_value AS FLOAT64) / exchange_source.ex_rate) _gbp_revenue,

/* Below macro creates additional fields based on form inputs for "Subaccounts, campaign delimitter, custom fields" */
{{add_fields("campaign")}} /* Replace with the report's campaign name field */

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM
/* source raw main table is referred as source_a and 
openexchange table is being referred as exchange_source */
  {{ref('googleads-campaign_performance-v1')}} source_a

LEFT JOIN exchange_source

ON

SAFE_CAST(source_a.day AS DATE) = exchange_source.day 
/* Jinja var if default field has null value..Replace the default field based on the report */
AND LOWER(IFNULL(TRIM(Currency),'{{var('account_currency')}}')) = exchange_source.currency_code 
/*When the currency value doesnt exist in the default report we add 
account currency variable by default */
