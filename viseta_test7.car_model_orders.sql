MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (unique_key id),
  cron '0 2 * * *',
  owner 'customer_analytics',
  tags ('car', 'orders', 'model', 'analytics'),
  audits (
    assert_not_null(user_id, 'user_id must not be null'),
    assert_not_null(car_id, 'car_id must not be null'),
    assert_not_null(name_en, 'name_en must not be null'),
    assert_not_null(order_id, 'order_id must not be null')
  ),
  description 'Calculates total orders and life time value per car model for Customer Service and Marketing teams',
  lookback 2
) AS
WITH cars_with_models AS (
  SELECT
    cars.id AS car_id,
    models.id AS model_id,
    models.name_en
  FROM cars
  JOIN models ON cars.id = models.id
),
orders_with_users_cars AS (
  SELECT
    orders.id AS order_id,
    orders.created_at,
    orders.grand_total,
    users.id AS user_id,
    cars_with_models.car_id,
    cars_with_models.model_id,
    cars_with_models.name_en
  FROM orders
  JOIN users ON orders.id = users.id
  JOIN cars_with_models ON users.id = cars_with_models.car_id
),
aggregated_orders AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY name_en) AS id,
    name_en,
    COUNT(order_id) AS count_of_orders,
    COALESCE(SUM(grand_total), 0) AS life_time_value,
    user_id,
    car_id,
    order_id
  FROM orders_with_users_cars
  WHERE user_id IS NOT NULL
    AND car_id IS NOT NULL
    AND name_en IS NOT NULL
    AND order_id IS NOT NULL
  GROUP BY name_en, user_id, car_id, order_id
)
SELECT
  id,
  name_en,
  count_of_orders,
  life_time_value
FROM aggregated_orders;