MODEL(
  dialect = 'ansi'
  kind INCREMENTAL_BY_UNIQUE_KEY unique_key user_id (lookback = 2)
  cron '0 2 * * *'
  owner 'customer_analytics'
  audits (
    must_have_data user_id,
    must_have_data created_at
  )
  tags (
    purpose 'Understand the lifetime value and total number of orders for each customer',
    users 'Customer Service team, Marketing team'
  )
) AS
WITH
  stage_orders AS (
    SELECT
      id AS order_id,
      user_id,
      grand_total,
      created_at
    FROM
      orders
    WHERE
      user_id IS NOT NULL
      AND created_at IS NOT NULL
  ),
  transform_ltv AS (
    SELECT
      user_id,
      COALESCE(SUM(grand_total), 0) AS life_time_value,
      COUNT(order_id) AS count_of_orders,
      MIN(created_at) AS first_order_date,
      MAX(created_at) AS last_order_date
    FROM
      stage_orders
    GROUP BY
      user_id
  ),
  final_output AS (
    SELECT
      user_id,
      life_time_value,
      count_of_orders,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_order_date DESC) AS order_rank,
      PERCENT_RANK() OVER (ORDER BY life_time_value) AS life_time_value_percent_rank,
      AVG(life_time_value) OVER (ORDER BY last_order_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS life_time_value_moving_avg,
      LAG(life_time_value, 1) OVER (PARTITION BY user_id ORDER BY last_order_date) AS life_time_value_lag_1,
      LAG(count_of_orders, 1) OVER (PARTITION BY user_id ORDER BY last_order_date) AS count_of_orders_lag_1
    FROM
      transform_ltv
  )
SELECT
  user_id,
  life_time_value,
  count_of_orders,
  life_time_value_percent_rank,
  life_time_value_moving_avg,
  life_time_value_lag_1,
  count_of_orders_lag_1
FROM
  final_output
WHERE
  order_rank = 1
;