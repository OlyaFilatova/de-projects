SELECT
  brand,
  ROUND(AVG(price), 2) AS avg_price
FROM products
GROUP BY brand
ORDER BY avg_price DESC
