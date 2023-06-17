DECLARE $bid AS Utf8;
DECLARE $pos AS Int32;
DECLARE $limit AS Int32;

SELECT b.val, b.pos
FROM zbytes b
WHERE b.bid=$bid AND b.pos > $pos
ORDER BY b.pos LIMIT $limit;
