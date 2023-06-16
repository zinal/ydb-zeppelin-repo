DECLARE $fid AS Utf8;

$qfile=(SELECT fid FROM zfile WHERE fid=$fid);

$qver=(SELECT fid, vid FROM zver WHERE fid=$fid);

$qbytes=(
SELECT b.bid AS bid, b.pos AS pos
FROM (SELECT bid FROM zver WHERE fid=$fid) v
INNER JOIN zbytes b ON b.bid=v.bid
);

DELETE FROM zbytes ON SELECT * FROM $qbytes;
DELETE FROM zver ON SELECT * FROM $qver;
DELETE FROM zfile ON SELECT * FROM $qfile;
