
WITH source_data AS (
    SELECT 
        PARSE_JSON(MESSAGE_BODY) AS raw_data
    FROM 
        BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_payload AS (
    SELECT 
        raw_data:"Payload"::OBJECT:"OfferChangeTrigger"::OBJECT:"ASIN"::STRING as ASIN,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"SalesRankings"::ARRAY as offer,
    FROM    
        source_data
),
flatten_payload_1 as (
    SELECT
        offer.value:"ProductCategoryId"::STRING as ProductCategoryId,
        offer.value:"Rank"::STRING as Ranking
        from flatten_payload, 
        lateral flatten(input=>flatten_payload.offer) as offer,
)
SELECT * 
FROM flatten_payload_1