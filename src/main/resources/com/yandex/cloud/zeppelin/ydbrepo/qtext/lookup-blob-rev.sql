DECLARE $fid AS Utf8;
DECLARE $vid AS Utf8;

SELECT v.bid
FROM zver v
WHERE v.fid=$fid AND v.vid=$vid;
