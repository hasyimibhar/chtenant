WITH regional AS (SELECT region, sum(amount) AS total FROM sales.orders GROUP BY region) SELECT region, total FROM regional ORDER BY region
