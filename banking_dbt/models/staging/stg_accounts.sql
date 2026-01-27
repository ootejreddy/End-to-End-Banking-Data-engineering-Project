{{ config(materialized='view') }}

WITH ranked AS (
  SELECT
    v:id::INT              AS account_id,
    v:customer_id::INT     AS customer_id,
    v:balance::FLOAT       AS balance,
    v:account_type::STRING AS account_type,
    v:currency::string      as currency,
    v:created_at::TIMESTAMP AS created_at,
    v:event_ts::TIMESTAMP_LTZ AS event_ts,
    v:lsn::NUMBER          AS lsn,
    v:op::STRING           AS op,

    ROW_NUMBER() OVER (
      PARTITION BY v:id::INT, v:lsn::NUMBER
      ORDER BY v:lsn::NUMBER DESC
    ) AS rn
  FROM {{ source('raw', 'accounts') }}
  WHERE v:op::STRING != 'r'
)

SELECT *
FROM ranked
WHERE rn = 1
  AND op != 'd'