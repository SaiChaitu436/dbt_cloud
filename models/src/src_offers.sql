with source_data as (
    select parse_json(MESSAGE_BODY) as raw_data
    from BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_payload as (
    select 
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING as NotificationId,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::OBJECT:"ASIN"::STRING as ASIN,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Offers"::ARRAY as offers
    from source_data
),
flatten_offers as (
    select NotificationId, ASIN, 
    row_number() over(partition by asin order by NotificationId desc) as OfferID,
    offers.value:"SellerId"::STRING as SellerId,
    offers.value:"IsBuyBoxWinner"::STRING as IsBuyBoxWinner,
    offers.value:"ListingPrice"::OBJECT:"Amount"::FLOAT as ListingPriceAmount,
    offers.value:"ListingPrice"::OBJECT:"CurrencyCode"::STRING as ListingCurrencyCode,
    offers.value:"PrimeInformation"::OBJECT:"IsOfferNationalPrime"::BOOLEAN as IsOfferNationalPrime,
    offers.value:"PrimeInformation"::OBJECT:"IsOfferPrime"::BOOLEAN as PrimeInformation,
    offers.value:"SubCondition"::STRING as SubCondition 
    from flatten_payload,
    lateral flatten(input=>flatten_payload.offers) as offers
)
SELECT
    *
FROM flatten_offers