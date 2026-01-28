{{ config(materialized='view') }}

SELECT
    v:id::string                 AS transaction_id,
    v:lsn::number                AS lsn,
    v:account_id::string         AS account_id,
    v:amount::float              AS amount,
    v:txn_type::string           AS transaction_type,
    v:related_account_id::string AS related_account_id,
    v:status::string             AS status,
    v:created_at::timestamp      AS transaction_time,
    TO_TIMESTAMP_LTZ(v:event_ts::NUMBER / 1000) AS event_ts,
    CURRENT_TIMESTAMP            AS load_timestamp,
    v:op::string                 AS op
FROM {{ source('raw', 'transactions') }}