SELECT * FROM (SELECT * FROM (SELECT * FROM analytics.events) inner_sub WHERE score > 10) outer_sub ORDER BY id
