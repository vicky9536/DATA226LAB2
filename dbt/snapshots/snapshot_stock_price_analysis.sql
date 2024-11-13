{% snapshot snapshot_session_summary %}
{{
    config(
        target_schema='snapshot',
        unique_key='symbol',
        strategy='timestamp',
        updated_at='date',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('stock_price_analysis') }}
{% endsnapshot %}