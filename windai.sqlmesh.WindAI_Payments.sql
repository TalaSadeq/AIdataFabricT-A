MODEL (
  name windai.sqlmesh.WindAI_Payments,
  owner 'customer_analytics',
  tags ('customer_analytics', 'financial_reporting'),
  grain 'user_id',
  cron '0 2 * * *',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key user_id,
    lookback 5
  ),
  audits (
    NOT_NULL(column = user_id)
  )
);

-- Staging layer: Select necessary columns from source tables
WITH stg_customers AS (
  SELECT
    c.customer_id
  FROM windai.sqlmesh.customers AS c
  WHERE c.customer_id IS NOT NULL
),

stg_orders AS (
  SELECT
    o.order_id,
    o.customer_id
  FROM windai.sqlmesh.orders AS o
  WHERE o.order_id IS NOT NULL
    AND o.customer_id IS NOT NULL
),

stg_payments AS (
  SELECT
    p.order_id,
    p.payment_value
  FROM windai.sqlmesh.order_payments AS p
  WHERE p.order_id IS NOT NULL
),

-- Transform layer: Aggregate order and payment data per customer
transform_customer_aggregates AS (
  SELECT
    so.customer_id,
    COUNT(DISTINCT so.order_id) AS count_of_orders,
    SUM(COALESCE(sp.payment_value, 0.0)) AS sum_of_orders
  FROM stg_orders AS so
  LEFT JOIN stg_payments AS sp
    ON so.order_id = sp.order_id
  GROUP BY
    so.customer_id
),

-- Final layer: Join customer data with aggregates and ensure all customers are included
final AS (
  SELECT
    sc.customer_id AS user_id,
    COALESCE(tca.count_of_orders, 0) AS count_of_orders,
    COALESCE(tca.sum_of_orders, 0.0) AS sum_of_orders
  FROM stg_customers AS sc
  LEFT JOIN transform_customer_aggregates AS tca
    ON sc.customer_id = tca.customer_id
)

-- Final SELECT statement
SELECT
  f.user_id,
  f.count_of_orders,
  f.sum_of_orders
FROM final AS f;