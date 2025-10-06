use salesfunnel;
select * from synthetic_online_retail_data;
RENAME TABLE synthetic_online_retail_data TO Synthetic_retail;
select * from synthetic_retail;
-- Validate schema and data types 
describe synthetic_retail;
-- Count of unique customers
SELECT 
    COUNT(DISTINCT customer_id) AS total_signups
FROM
    Synthetic_retail;
    
   -- Total orders
   SELECT COUNT(*) AS total_purchases FROM Synthetic_retail;
   
   -- Conversion rate
   SELECT 
  COUNT(*) / COUNT(DISTINCT customer_id) AS purchase_per_customer_ratio
FROM Synthetic_retail;

-- ARPU- Average Revenue Per User
SELECT 
  AVG(total_revenue) AS ARPU
FROM (
  SELECT customer_id, SUM(price * quantity) AS total_revenue
  FROM Synthetic_retail
  GROUP BY customer_id
) t;

-- Creating a new table extended synthetic to do 30/60/90 retention
CREATE TABLE simulated_repeats AS 
SELECT 
  customer_id,
  DATE_ADD(STR_TO_DATE(order_date, '%m/%d/%Y'), INTERVAL 30 DAY) AS order_date,
  product_id,
  category_id,
  category_name,
  product_name,
  quantity,
  price,
  payment_method,
  city,
  review_score,
  gender,
  age
FROM Synthetic_retail
WHERE customer_id IN (
  SELECT customer_id 
  FROM Synthetic_retail
  GROUP BY customer_id
  HAVING COUNT(*) = 1
)
AND RAND() < 0.3 

UNION ALL

SELECT 
  customer_id,
  DATE_ADD(STR_TO_DATE(order_date, '%m/%d/%Y'), INTERVAL 60 DAY) AS order_date,
  product_id,
  category_id,
  category_name,
  product_name,
  quantity,
  price,
  payment_method,
  city,
  review_score,
  gender,
  age
FROM Synthetic_retail
WHERE customer_id IN (
  SELECT customer_id 
  FROM Synthetic_retail
  GROUP BY customer_id
  HAVING COUNT(*) = 1
)
AND RAND() < 0.2

UNION ALL

SELECT 
  customer_id,
  DATE_ADD(STR_TO_DATE(order_date, '%m/%d/%Y'), INTERVAL 90 DAY) AS order_date,
  product_id,
  category_id,
  category_name,
  product_name,
  quantity,
  price,
  payment_method,
  city,
  review_score,
  gender,
  age
FROM Synthetic_retail
WHERE customer_id IN (
  SELECT customer_id 
  FROM Synthetic_retail
  GROUP BY customer_id
  HAVING COUNT(*) = 1
)
AND RAND() < 0.1;


CREATE TABLE extended_synthetic_retail AS
SELECT customer_id, STR_TO_DATE(order_date, '%m/%d/%Y') AS order_date, product_id, category_id, category_name, product_name, quantity, price, payment_method, city, review_score, gender, age
FROM Synthetic_retail
UNION ALL
SELECT customer_id, order_date, product_id, category_id, category_name, product_name, quantity, price, payment_method, city, review_score, gender, age
FROM simulated_repeats;

-- Customer retention 30 days
WITH first_purchase AS (
  SELECT customer_id, MIN(order_date) AS first_order_date
  FROM extended_synthetic_retail
  GROUP BY customer_id
),
repeat_purchase AS (
  SELECT s.customer_id
  FROM extended_synthetic_retail s
  JOIN first_purchase f ON s.customer_id = f.customer_id
  WHERE s.order_date > f.first_order_date 
    AND s.order_date <= DATE_ADD(f.first_order_date, INTERVAL 30 DAY)
)
SELECT 
  COUNT(DISTINCT repeat_purchase.customer_id) / COUNT(DISTINCT first_purchase.customer_id) AS retention_30_days
FROM first_purchase
LEFT JOIN repeat_purchase ON first_purchase.customer_id = repeat_purchase.customer_id;

-- Customer retention 60 days
WITH first_purchase AS (
  SELECT customer_id, MIN(order_date) AS first_order_date
  FROM extended_synthetic_retail
  GROUP BY customer_id
),
repeat_purchase AS (
  SELECT s.customer_id
  FROM extended_synthetic_retail s
  JOIN first_purchase f ON s.customer_id = f.customer_id
  WHERE s.order_date > f.first_order_date 
    AND s.order_date <= DATE_ADD(f.first_order_date, INTERVAL 60 DAY)
)
SELECT 
  COUNT(DISTINCT repeat_purchase.customer_id) / COUNT(DISTINCT first_purchase.customer_id) AS retention_60_days
FROM first_purchase
LEFT JOIN repeat_purchase ON first_purchase.customer_id = repeat_purchase.customer_id;

-- Customer retention 90 days 
WITH first_purchase AS (
  SELECT customer_id, MIN(order_date) AS first_order_date
  FROM extended_synthetic_retail
  GROUP BY customer_id
),
repeat_purchase AS (
  SELECT s.customer_id
  FROM extended_synthetic_retail s
  JOIN first_purchase f ON s.customer_id = f.customer_id
  WHERE s.order_date > f.first_order_date 
    AND s.order_date <= DATE_ADD(f.first_order_date, INTERVAL 90 DAY)
)
SELECT 
  COUNT(DISTINCT repeat_purchase.customer_id) / COUNT(DISTINCT first_purchase.customer_id) AS retention_90_days
FROM first_purchase
LEFT JOIN repeat_purchase ON first_purchase.customer_id = repeat_purchase.customer_id;

-- ARPU- Avg. Revenue Per user 
SELECT 
  AVG(total_revenue) AS ARPU
FROM (
  SELECT customer_id, SUM(price * quantity) AS total_revenue
  FROM extended_synthetic_retail
  GROUP BY customer_id
) t;

-- LTV
   SELECT 
  customer_id,
  SUM(price * quantity) AS lifetime_value
FROM extended_synthetic_retail
GROUP BY customer_id;

-- AVG lifetime value 

SELECT 
  AVG(lifetime_value) AS average_ltv
FROM (
  SELECT 
    customer_id,
    SUM(price * quantity) AS lifetime_value
  FROM extended_synthetic_retail
  GROUP BY customer_id
) t;

-- Funnel Counts (Leads proxy, Sign-ups, Purchases):
SELECT 
  city AS lead_proxy,
  COUNT(DISTINCT customer_id) AS signups,
  COUNT(*) AS purchases
FROM extended_synthetic_retail
GROUP BY city
ORDER BY purchases DESC;

-- Conversion Rates per city: 
 WITH funnel AS (
  SELECT 
    city,
    COUNT(DISTINCT customer_id) AS signups,
    COUNT(*) AS purchases
  FROM extended_synthetic_retail
  GROUP BY city
)
SELECT 
  city,
  signups,
  purchases,
  ROUND(purchases/signups, 2) AS purchase_conversion_rate
FROM funnel;

-- ARPU by city:
SELECT 
  city,
  AVG(total_revenue) AS ARPU
FROM (
  SELECT 
    customer_id,
    city,
    SUM(price * quantity) AS total_revenue
  FROM extended_synthetic_retail
  GROUP BY customer_id, city
) t
GROUP BY city;

-- Retention (30-day) by city:
WITH first_purchase AS (
  SELECT customer_id, city, MIN(order_date) AS first_order_date
  FROM extended_synthetic_retail
  GROUP BY customer_id, city
),
repeat_purchase AS (
  SELECT s.customer_id, s.city
  FROM extended_synthetic_retail s
  JOIN first_purchase f ON s.customer_id = f.customer_id
  WHERE s.order_date > f.first_order_date AND s.order_date <= DATE_ADD(f.first_order_date, INTERVAL 30 DAY)
)
SELECT 
  f.city,
  COUNT(DISTINCT repeat_purchase.customer_id) / COUNT(DISTINCT f.customer_id) AS retention_30_days
FROM first_purchase f
LEFT JOIN repeat_purchase ON f.customer_id = repeat_purchase.customer_id AND f.city = repeat_purchase.city
GROUP BY f.city;

 

