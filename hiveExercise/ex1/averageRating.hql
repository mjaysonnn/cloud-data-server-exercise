SELECT movieid, AVG(rating) AS average FROM ratings GROUP BY movieid ORDER BY movieid DESC LIMIT 10;
