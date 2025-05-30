{{
  config(
    alias= 'googleads-ad_performance_conversions_pmax-v1'
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
    SAFE_CAST(source_a.Day AS DATE)	day,
    SAFE_CAST(Hour_of_day AS STRING) hour_of_day,
    SAFE_CAST(Advertising_channel AS STRING) advertising_channel,
    SAFE_CAST(All_conversion_value__by_conversion_time_ AS FLOAT64)	all_conversion_value_by_conversion_time,
    SAFE_CAST(All_conversions AS FLOAT64) all_conversions,
    SAFE_CAST(All_conversions__by_conversion_time_ AS FLOAT64) all_conversions_by_conversion_time,
    SAFE_CAST(All_conversions_value AS FLOAT64)	all_conversions_value,
    SAFE_CAST(Campaign AS STRING) campaign_name,
    SAFE_CAST(Campaign_ID AS STRING) campaign_id,
    SAFE_CAST(Conversion_name AS STRING) conversion_name,
    SAFE_CAST(Conversion_value__by_conversion_time_ AS FLOAT64)	conversion_value_by_conversion_time,
    SAFE_CAST(Conversions AS FLOAT64) conversions,
    SAFE_CAST(Conversions__by_conversion_time_ AS FLOAT64) conversions_by_conversion_time,
    SAFE_CAST(Customer_ID AS STRING) customer_id,
    SAFE_CAST(Customer_name AS STRING) customer_name,
    SAFE_CAST(Device AS STRING)	device,
    SAFE_CAST(MCC_ID AS STRING)	mcc_id,
    SAFE_CAST(MCC_name AS STRING) mcc_name,
    SAFE_CAST(Time_zone AS STRING) time_zone,
    SAFE_CAST(Total_conversion_value AS FLOAT64) total_conversion_value,
    SAFE_CAST(View_through_conversions AS INT64) view_through_conversions,
    SAFE_CAST(TRIM(Currency) AS STRING)	currency,
'googleads-ad_performance_conversions_pmax-v1' AS raw_origin,
    (SAFE_CAST(Conversion_value__by_conversion_time_ AS FLOAT64) / exchange_source.ex_rate) _gbp_revenue,

    {{add_fields("campaign")}} /* Replace with the report's campaign name field */

FROM
  {{ source('raw_main','googleads-ad_performance_conversions_pmax_v_1') }} source_a

LEFT JOIN exchange_source

ON

    SAFE_CAST(source_a.Day AS DATE) = exchange_source.day -- source_a.{datecolumn}

/* Jinja var if default field has null value..Replace the default field based on the report */

    AND LOWER(IFNULL(TRIM(currency), '{{var('account_currency')}}')) = exchange_source.currency_code  --using TRIM to get rid of trailing whitespace

