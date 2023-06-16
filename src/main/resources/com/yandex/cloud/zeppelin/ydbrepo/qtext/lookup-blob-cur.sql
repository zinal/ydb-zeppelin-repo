DECLARE $fid AS Utf8;

SELECT v.bid
FROM (SELECT fid, vid FROM zfile WHERE fid=$fid) AS f
INNER JOIN zver v ON v.fid=f.fid AND v.vid=f.vid;
