MODEL (
  name windai.sqlmesh.WindAI_Payments,
  cron '0 2 * * *',
  grain customer_id,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (customer_id),
    lookback 5
  )
);

-- Stage customers data with required validation
WITH customers_staged AS (
  SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
  FROM customers
  WHERE
    customer_id IS NOT NULL
),

-- Stage orders data with customer relationships
orders_staged AS (
  SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_customer_date
  FROM orders
  WHERE
    customer_id IS NOT NULL
    AND order_id IS NOT NULL
),

-- Calculate payment values per order
order_payments_agg AS (
  SELECT
    order_id,
    SUM(COALESCE(payment_value, 0)) AS total_payment_value,
    COUNT(DISTINCT payment_sequential) AS payment_count
  FROM order_payments
  WHERE
    order_id IS NOT NULL
  GROUP BY
    order_id
),

-- Transform: Join orders with payments to get enriched order data
orders_with_payments AS (
  SELECT
    os.customer_id,
    os.order_id,
    os.order_status,
    os.order_purchase_timestamp,
    COALESCE(opa.total_payment_value, 0) AS order_payment_value,
    COALESCE(opa.payment_count, 0) AS order_payment_count
  FROM orders_staged AS os
  LEFT JOIN order_payments_agg AS opa
    ON os.order_id = opa.order_id
),

-- Final aggregation: Calculate customer-level metrics
final_customer_metrics AS (
  SELECT
    cs.customer_id,
    cs.customer_unique_id,
    cs.customer_zip_code_prefix,
    cs.customer_city,
    cs.customer_state,
    SUM(owp.order_payment_value) AS sum_of_orders,
    COUNT(DISTINCT owp.order_id) AS count_of_orders,
    AVG(owp.order_payment_value) AS avg_order_value,
    MAX(owp.order_purchase_timestamp) AS last_order_date,
    MIN(owp.order_purchase_timestamp) AS first_order_date
  FROM customers_staged AS cs
  LEFT JOIN orders_with_payments AS owp
    ON cs.customer_id = owp.customer_id
  GROUP BY
    cs.customer_id,
    cs.customer_unique_id,
    cs.customer_zip_code_prefix,
    cs.customer_city,
    cs.customer_state
)

-- Select final output with proper NULL handling
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  COALESCE(sum_of_orders, 0) AS sum_of_orders,
  COALESCE(count_of_orders, 0) AS count_of_orders,
  COALESCE(avg_order_value, 0) AS avg_order_value,
  last_order_date,
  first_order_date
FROM final_customer_metrics;