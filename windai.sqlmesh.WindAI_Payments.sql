MODEL(
  name windai.sqlmesh.WindAI_Payments,
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key (customer_id), lookback 5),
  cron "0 2 * * *",
  grain "One customer (user)"
);

WITH stage_orders AS (
  SELECT
    customer_id,
    order_id
  FROM windai.sqlmesh.orders
  WHERE customer_id IS NOT NULL
),

transform_customer_orders AS (
  SELECT
    so.customer_id,
    COUNT(DISTINCT so.order_id) AS total_orders
  FROM stage_orders so
  GROUP BY so.customer_id
),

final AS (
  SELECT
    c.customer_id,
    COALESCE(tco.total_orders, 0) AS total_orders,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state
  FROM windai.sqlmesh.customers c
  LEFT JOIN transform_customer_orders tco
    ON c.customer_id = tco.customer_id
  WHERE c.customer_id IS NOT NULL
)

SELECT
  customer_id,
  total_orders,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM final;