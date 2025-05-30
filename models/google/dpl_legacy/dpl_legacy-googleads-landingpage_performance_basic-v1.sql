{{
  config(
    alias= 'googleads-landingpage_performance_basic-v1'
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
    SAFE_CAST( Advertising_channel AS STRING ) advertising_channel,
    SAFE_CAST( Campaign AS STRING ) campaign_name,
    SAFE_CAST( Campaign_ID AS STRING ) campaign_id,
    SAFE_CAST( Clicks AS INT64 ) clicks,
    SAFE_CAST( Cost AS FLOAT64 ) cost,
    SAFE_CAST( Currency AS STRING ) currency,
    SAFE_CAST( Customer_ID AS STRING ) customer_id,
    SAFE_CAST( Customer_name AS STRING ) customer_name,
    SAFE_CAST( source_a.Day AS DATE ) day,
    SAFE_CAST( Engagements AS INT64 ) engagements,
    SAFE_CAST( Impressions AS INT64 ) impressions,
    SAFE_CAST( Interactions AS INT64 ) interactions,
    SAFE_CAST( Landing_page AS STRING ) landing_page,
    SAFE_CAST( Mobile_speed_score AS FLOAT64 ) mobile_speed_score,
    SAFE_CAST( Views AS INT64 ) views,
    SAFE_CAST( All_conversions_value AS FLOAT64 ) all_conversions_value,
'googleads-landingpage_performance_basic-v1' AS raw_origin,

(SAFE_CAST( Cost AS FLOAT64) / exchange_source.ex_rate ) _gbp_cost,
(SAFE_CAST( All_conversion_value AS FLOAT64 ) / exchange_source.ex_rate ) _gbp_revenue, --value or cost???

/* Below macro creates additional fields based on form inputs for "Subaccounts, campaign delimitter, custom fields" */
{{add_fields("Campaign")}} /* Replace with the report's campaign name field */

/* *****Please Do not remove the backticks from the query as it is essential for compiling queries with hyphen in DBT**** */

FROM
/* source raw main table is referred as source_a and 
openexchange table is being referred as exchange_source */
  {{ source('raw_main', 'googleads-landingpage_performance_basic_v_1') }} source_a

LEFT JOIN exchange_source

ON

SAFE_CAST(source_a.day AS DATE) = exchange_source.day 
/* Jinja var if default field has null value..Replace the default field based on the report */
AND LOWER(IFNULL(TRIM(Currency),'{{var('account_currency')}}')) = exchange_source.currency_code 
/*When the currency value doesnt exist in the default report we add 
account currency variable by default */


