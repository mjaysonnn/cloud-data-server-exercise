SELECT COUNT(tag) AS occurrence, tag FROM tags GROUP BY tag ORDER BY occurrence DESC LIMIT 1;
