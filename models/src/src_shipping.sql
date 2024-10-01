with source_data as (
    select parse_json(MESSAGE_BODY) as raw_data 
    from BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_payload as (
    select 
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Offers"::ARRAY as offer
    from source_data
),

flatten_data as (
    SELECT
    NotificationId,
    offers.value:"Shipping"::OBJECT:"Amount"::INTEGER as ShippingAmount,
    offers.value:"Shipping"::OBJECT:"CurrencyCode"::STRING as ShippingCurrencyCode,
    offers.value:"ShippingTime"::OBJECT:"AvailabilityType"::STRING as ShippingAvailabilityType,
    offers.value:"ShippingTime"::OBJECT:"AvailableDate"::STRING as ShippingAvailabilityDate,
    offers.value:"ShippingTime"::OBJECT:"MaximumHours"::FLOAT as ShippingMaximumHours,
    offers.value:"ShippingTime"::OBJECT:"MinimumHours"::FLOAT as ShippingMinimumHours,
    offers.value:"ShipsDomestically"::BOOLEAN as ShipsDomestically,

    FROM    
        flatten_payload,
    LATERAL FLATTEN(input=>flatten_payload.offer) AS offers
)
select * from flatten_data