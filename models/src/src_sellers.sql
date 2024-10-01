{{ config(materialized='incremental',unique_key = 'SellerId', merge_update_columns = ['IsFeaturedMerchant','IsFulfilledByAmazon']) }}

with source_data as (
    select parse_json(MESSAGE_BODY) as raw_data from 
    {{ source('buy_box','buybox') }}
    -- BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_payload as (
    select 
    --Extra added one's
    raw_data:"EventTime"::STRING AS EventTime,

    raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Offers"::ARRAY as offers
    from source_data
)
select 
    row_number() over (order by(select null)) as id,
    offer.value:"SellerId"::STRING as SellerId, 
    offer.value:"IsFeaturedMerchant"::BOOLEAN as IsFeaturedMerchant,
    offer.value:"IsFulfilledByAmazon"::BOOLEAN as IsFulfilledByAmazon,
    EventTime,
    current_timestamp() as updated_at,
    md5(concat(offer.value:"SellerId",EventTime,IsFeaturedMerchant,IsFulfilledByAmazon,id)) as surrogate_key
from flatten_payload f,
lateral flatten(input=>f.offers) as offer