SELECT u.name, sum(o.amount) AS total FROM shop.users u JOIN shop.orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name
