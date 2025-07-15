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
    models.id AS model_id,
    models.name_en
  FROM cars
  JOIN models ON cars.id = models.id
),
orders_users_cars AS (
  SELECT
    orders.id AS order_id,
    orders.created_at,
    orders.grand_total,
    orders.user_id,
    orders.car_id,
    cars_models.name_en
  FROM orders
  JOIN cars_models ON orders.car_id = cars_models.car_id
  WHERE orders.user_id IS NOT NULL
    AND orders.car_id IS NOT NULL
),
final_agg AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY name_en) AS id,
    name_en,
    COUNT(order_id) AS count_of_orders,
    COALESCE(SUM(grand_total), 0) AS life_time_value
  FROM orders_users_cars
  WHERE name_en IS NOT NULL
    AND order_id IS NOT NULL
  GROUP BY name_en
)
SELECT
  id,
  name_en,
  count_of_orders,
  life_time_value
FROM final_agg;