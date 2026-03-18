SELECT t1.id, t1.val FROM (SELECT id FROM db1.t1 WHERE id <= 2) sub, db1.t1 WHERE t1.id = sub.id ORDER BY t1.id
