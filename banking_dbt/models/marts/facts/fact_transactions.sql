{{ config(materialized='incremental', unique_key=['transaction_id', 'lsn']) }}


WITH deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY transaction_id, lsn
           ORDER BY event_ts DESC
         ) AS rn
  FROM {{ ref('stg_transactions') }}
  WHERE op != 'd'
)

SELECT
    t.transaction_id,
    t.lsn,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    t.event_ts,
    CURRENT_TIMESTAMP AS load_timestamp,
    CASE
        WHEN transaction_type = 'DEPOSIT' THEN amount
        WHEN transaction_type = 'WITHDRAWAL' THEN -amount
        WHEN transaction_type = 'TRANSFER' THEN -amount
        ELSE amount
    END AS amount_signed,
FROM deduped t
LEFT JOIN {{ ref('stg_accounts') }} a
    ON t.account_id = a.account_id