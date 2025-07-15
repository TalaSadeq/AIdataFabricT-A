MODEL (
  name windai.sqlmesh.WindAI_Payments,
  owner 'customer_analytics',
  tags ('financial', 'customer_analytics'),
  grain 'user_id',
  cron '0 2 * * *',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key user_id,
    lookback 5
  )
);

-- Stage layer: Extract and filter base data from source tables
WITH stg_customers AS (
  SELECT
    customer_id
  FROM windai.sqlmesh.customers
  WHERE
    customer_id IS NOT NULL
),

stg_orders AS (
  SELECT
    order_id,
    customer_id
  FROM windai.sqlmesh.orders
  WHERE
    customer_id IS NOT NULL
    AND order_id IS NOT NULL
),

stg_payments AS (
  SELECT
    order_id,
    payment_value
  FROM windai.sqlmesh.order_payments
  WHERE
    order_id IS NOT NULL
),

-- Transform layer: Calculate order metrics per customer
transform_customer_metrics AS (
  SELECT
    o.customer_id,
    -- Count unique orders per customer
    COUNT(DISTINCT o.order_id) AS count_of_orders,
    -- Sum payment values for all customer orders
    SUM(COALESCE(p.payment_value, 0.0)) AS sum_of_orders
  FROM stg_orders AS o
  LEFT JOIN stg_payments AS p
    ON o.order_id = p.order_id
  GROUP BY
    o.customer_id
),

-- Final layer: Ensure all customers are included with complete metrics
final AS (
  SELECT
    c.customer_id AS user_id,
    COALESCE(tcm.count_of_orders, 0) AS count_of_orders,
    COALESCE(tcm.sum_of_orders, 0.0) AS sum_of_orders
  FROM stg_customers AS c
  LEFT JOIN transform_customer_metrics AS tcm
    ON c.customer_id = tcm.customer_id
)

SELECT
  user_id,
  count_of_orders,
  sum_of_orders
FROM final;