DECLARE $fid AS Utf8;
DECLARE $pos AS Int32;
DECLARE $limit AS Int32;

SELECT b.val, b.pos
FROM (SELECT vid FROM zfile WHERE fid=$fid) AS f
INNER JOIN zver v ON v.fid=f.fid AND v.vid=f.vid
INNER JOIN zbytes b ON b.bid=v.bid
WHERE b.pos > $pos
ORDER BY b.pos LIMIT $limit;
