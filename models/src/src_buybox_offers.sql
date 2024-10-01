WITH source_data AS (
    SELECT 
        PARSE_JSON(MESSAGE_BODY) AS raw_data
    FROM 
        BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_payload AS (
    SELECT 
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"NumberOfOffers"::ARRAY as offer,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"NumberOfBuyBoxEligibleOffers"::ARRAY as offers
    FROM    
        source_data
),
flatten_payload_1 as (
    SELECT
        offers.value:"Condition"::STRING as EligibleOfferCondition,
        offers.value:"FulfillmentChannel"::STRING as EligibleFulFillmentChannel,
        offers.value:"OfferCount"::STRING as EligibleOfferCount,
        
        offer.value:"Condition"::STRING as NumberOfferCondition,
        offer.value:"FulfillmentChannel"::STRING as NumberFulFillmentChannel,
        offer.value:"OfferCount"::STRING as NumberOfferCount,

        
        from flatten_payload, 
        lateral flatten(input=>flatten_payload.offer) as offer,
        lateral flatten(input=>flatten_payload.offers) as offers
)
SELECT * 
FROM flatten_payload_1