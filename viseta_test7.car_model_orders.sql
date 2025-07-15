MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY unique_key id lookback 2,
  owner 'customer_analytics',
  cron '0 2 * * *',
  tags ('car', 'orders', 'model', 'analytics'),
  audits (
    assert_not_null(user_id, 'user_id must not be null'),
    assert_not_null(car_id, 'car_id must not be null'),
    assert_not_null(name_en, 'name_en must not be null'),
    assert_not_null(order_id, 'order_id must not be null')
  ),
  description 'Generates total orders count and sum of grand_total (life_time_value) per car model (models.name_en) for Customer Service and Marketing teams'
) AS
WITH cars_models AS (
  SELECT
    cars.id AS car_id,
    cars.model_id,
    models.name_en
  FROM cars
  INNER JOIN models ON cars.model_id = models.id
  WHERE cars.id IS NOT NULL
    AND cars.model_id IS NOT NULL
    AND models.name_en IS NOT NULL
),
valid_orders AS (
  SELECT
    orders.id AS order_id,
    orders.created_at,
    orders.grand_total,
    orders.user_id,
    orders.car_id
  FROM orders
  WHERE orders.id IS NOT NULL
    AND orders.car_id IS NOT NULL
    AND orders.user_id IS NOT NULL
),
final_aggregation AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY cm.name_en) AS id,
    cm.name_en,
    COUNT(vo.order_id) AS count_of_orders,
    COALESCE(SUM(vo.grand_total), 0) AS life_time_value
  FROM valid_orders vo
  INNER JOIN cars_models cm ON vo.car_id = cm.car_id
  GROUP BY cm.name_en
)
SELECT
  id,
  name_en,
  count_of_orders,
  life_time_value
FROM final_aggregation;