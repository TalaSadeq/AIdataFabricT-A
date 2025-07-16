MODEL (
  name windai.sqlmesh.WindAI_Payments,
  owner customer_analytics,
  cron "0 2 * * *",
  grain customer_id,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key = (customer_id),
    lookback = 5
  )
);

-- Stage: Identify customers with recent activity to update their lifetime metrics
WITH customers_with_recent_activity AS (
  SELECT DISTINCT
    customer_id
  FROM windai.sqlmesh.orders
  WHERE
    order_purchase_timestamp >= @start_ds - INTERVAL '5' DAY
    AND customer_id IS NOT NULL
),

-- Stage: Get all historical orders for these customers
all_customer_orders AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_status
  FROM windai.sqlmesh.orders o
  INNER JOIN customers_with_recent_activity cra
    ON o.customer_id = cra.customer_id
  WHERE
    o.customer_id IS NOT NULL
    AND o.order_status IN ('delivered', 'shipped', 'processing', 'approved')
),

-- Stage: Calculate payment totals for relevant orders
order_payment_totals AS (
  SELECT
    op.order_id,
    SUM(COALESCE(op.payment_value, 0)) AS total_payment_value
  FROM windai.sqlmesh.order_payments op
  INNER JOIN all_customer_orders aco
    ON op.order_id = aco.order_id
  GROUP BY
    op.order_id
),

-- Transform: Join orders with their payment totals
transformed_orders AS (
  SELECT
    aco.customer_id,
    aco.order_id,
    aco.order_purchase_timestamp,
    COALESCE(opt.total_payment_value, 0) AS order_value
  FROM all_customer_orders aco
  LEFT JOIN order_payment_totals opt
    ON aco.order_id = opt.order_id
),

-- Final: Calculate customer lifetime metrics
final AS (
  SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS count_of_orders,
    SUM(order_value) AS sum_of_orders,
    MAX(order_purchase_timestamp) AS last_order_date,
    AVG(order_value) AS avg_order_value,
    MIN(order_purchase_timestamp) AS first_order_date
  FROM transformed_orders
  GROUP BY
    customer_id
)

SELECT
  customer_id,
  count_of_orders,
  sum_of_orders,
  last_order_date,
  avg_order_value,
  first_order_date
FROM final;