WITH source_data AS (
    SELECT 
        PARSE_JSON(MESSAGE_BODY) AS raw_data
    FROM 
        BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_payload AS (
    SELECT 
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::ARRAY as offer,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"BuyBoxPrices"::ARRAY as offers
    FROM    
        source_data
),
flatten_payload_1 as (
    SELECT
        NotificationId,
        offers.value:"LandedPrice"::Object:"Amount"::FLOAT as LandedPriceAmount,
        offers.value:"LandedPrice"::Object:"CurrencyCode"::STRING as LandedCurrencyCode,
        
        offers.value:"ListingPrice"::Object:"Amount"::FLOAT as ListingPriceAmount,
        offer.value:"ListPrice"::Object:"CurrencyCode"::STRING as ListingCurrencyCode,
        
        offers.value:"Shipping"::OBJECT:"Amount"::FLOAT as ShippingCost,
        offers.value:"Shipping"::OBJECT:"CurrencyCode"::STRING as ShippingCode,

        offer.value:"ListPrice"::OBJECT:"Amount"::FLOAT as ListPriceAmount
        
        from flatten_payload, 
        lateral flatten(input=>flatten_payload.offer) as offer,
        lateral flatten(input=>flatten_payload.offers) as offers
)
SELECT * 
FROM flatten_payload_1