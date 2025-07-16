MODEL (
  name windai.sqlmesh.WindAI_Payments,
  owner customer_analytics,
  cron '0 2 * * *',
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key = (customer_id),
    lookback = 5
  ),
  grain "One customer (user)"
);

/*
This model aggregates order and payment data to understand customer needs
and provide total order counts and payment values per customer.
Used by Financial Team and Marketing team for customer analytics.
*/

-- Stage: Extract base customer and order data
WITH stage_customers AS (
  SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
  FROM windai.sqlmesh.customers
  WHERE customer_id IS NOT NULL
),

-- Stage: Get delivered orders within lookback window
stage_orders AS (
  SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at
  FROM windai.sqlmesh.orders
  WHERE
    order_status = 'delivered'
    AND customer_id IS NOT NULL
    AND order_purchase_timestamp >= @start_date
    AND order_purchase_timestamp <= @end_date
),

-- Stage: Get payment data for orders
stage_payments AS (
  SELECT
    order_id,
    payment_type,
    payment_installments,
    payment_value
  FROM windai.sqlmesh.order_payments
  WHERE
    order_id IS NOT NULL
    AND payment_value IS NOT NULL
),

-- Transform: Join orders with payments to get order-level metrics
transform_order_payments AS (
  SELECT
    so.customer_id,
    so.order_id,
    so.order_purchase_timestamp,
    COALESCE(sp.payment_value, 0) AS order_payment_value,
    sp.payment_type,
    sp.payment_installments
  FROM stage_orders so
  LEFT JOIN stage_payments sp
    ON so.order_id = sp.order_id
),

-- Transform: Calculate customer-level aggregations
transform_customer_metrics AS (
  SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS count_of_orders,
    SUM(order_payment_value) AS sum_of_orders,
    AVG(order_payment_value) AS avg_order_value,
    MAX(order_purchase_timestamp) AS most_recent_order_date,
    MIN(order_purchase_timestamp) AS first_order_date,
    COUNT(DISTINCT CASE WHEN payment_type = 'credit_card' THEN order_id END) AS credit_card_orders,
    COUNT(DISTINCT CASE WHEN payment_installments > 1 THEN order_id END) AS installment_orders
  FROM transform_order_payments
  GROUP BY customer_id
),

-- Final: Join customer info with aggregated metrics
final AS (
  SELECT
    sc.customer_id,
    sc.customer_unique_id,
    sc.customer_zip_code_prefix,
    sc.customer_city,
    sc.customer_state,
    COALESCE(tcm.count_of_orders, 0) AS count_of_orders,
    COALESCE(tcm.sum_of_orders, 0) AS sum_of_orders,
    COALESCE(tcm.avg_order_value, 0) AS avg_order_value,
    tcm.most_recent_order_date,
    tcm.first_order_date,
    COALESCE(tcm.credit_card_orders, 0) AS credit_card_orders,
    COALESCE(tcm.installment_orders, 0) AS installment_orders,
    CASE
      WHEN tcm.count_of_orders >= 5 THEN 'High Value'
      WHEN tcm.count_of_orders >= 2 THEN 'Medium Value'
      WHEN tcm.count_of_orders >= 1 THEN 'Low Value'
      ELSE 'No Orders'
    END AS customer_segment
  FROM stage_customers sc
  LEFT JOIN transform_customer_metrics tcm
    ON sc.customer_id = tcm.customer_id
)

SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  count_of_orders,
  sum_of_orders,
  avg_order_value,
  most_recent_order_date,
  first_order_date,
  credit_card_orders,
  installment_orders,
  customer_segment
FROM final;