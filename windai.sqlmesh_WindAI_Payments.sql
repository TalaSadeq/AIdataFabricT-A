MODEL(
  name windai.sqlmesh.WindAI_Payments
  owner_team customer_analytics
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key = (customer_id),
    lookback = 5
  )
  cron "0 2 * * *"
  grain "One customer (user)"
)

-- Stage: Prepare customers and orders with early null checks
WITH stage_customers AS (
  SELECT
    customer_id,
    COALESCE(customer_unique_id, customer_id) AS user_id
  FROM windai.sqlmesh.customers
  WHERE customer_id IS NOT NULL
),
stage_orders AS (
  SELECT
    order_id,
    customer_id,
    order_purchase_timestamp
  FROM windai.sqlmesh.orders
  WHERE order_id IS NOT NULL AND customer_id IS NOT NULL
),

-- Transform: Join customers to orders
transform_customer_orders AS (
  SELECT
    c.customer_id,
    c.user_id,
    o.order_id,
    o.order_purchase_timestamp
  FROM stage_customers c
  INNER JOIN stage_orders o
    ON c.customer_id = o.customer_id
),

-- Final aggregation: count orders per customer, display user_id and first/last order dates
final AS (
  SELECT
    customer_id,
    user_id,
    COUNT(order_id) AS count_of_orders,
    MIN(order_purchase_timestamp) AS first_order_date,
    MAX(order_purchase_timestamp) AS last_order_date
  FROM transform_customer_orders
  GROUP BY customer_id, user_id
)

SELECT
  customer_id,
  user_id,
  count_of_orders,
  first_order_date,
  last_order_date
FROM final;