with source_data as (
    select parse_json(MESSAGE_BODY) as raw_data from 
    BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_payload as (
    select raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::ARRAY as offer,
    raw_data:"NotificationMetadata"::OBJECT:"PublishTime"::STRING as PublishTime
    from source_data
)
select 
    offer.value:"ASIN"::STRING as ASIN,
    offer.value:"ItemCondition"::STRING as ItemCondition,
    PublishTime
    
from flatten_payload f,
lateral flatten(input=>f.offer) as offer