{% snapshot scd_sellers %}
{{
  config(
    target_schema='DEV',
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True,
    merge_columns =['surrogate_key']

  )
}}

SELECT * 
FROM BUY_BOX.DEV.SRC_SELLERS

{% endsnapshot %}
