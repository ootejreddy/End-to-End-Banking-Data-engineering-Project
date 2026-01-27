{{ config(materialized='view') }}

WITH ranked AS (
  SELECT
    v:id::INT            AS customer_id,
    v:first_name::STRING AS first_name,
    v:last_name::STRING  AS last_name,
    v:email::STRING      AS email,
    v:created_at::TIMESTAMP AS created_at,
    v:event_ts::TIMESTAMP_LTZ AS event_ts,
    v:lsn::NUMBER        AS lsn,
    v:op::STRING         AS op,

    ROW_NUMBER() OVER (
      PARTITION BY v:id::INT, v:lsn::NUMBER
      ORDER BY v:lsn::NUMBER DESC
    ) AS rn
  FROM {{ source('raw', 'customers') }}
  WHERE v:op::STRING != 'r'
)

SELECT *
FROM ranked
WHERE rn = 1    