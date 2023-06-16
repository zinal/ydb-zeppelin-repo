DECLARE $fid AS Utf8;

$qfile=(SELECT fid FROM zfile WHERE fid=$fid);

$qver=(SELECT fid, vid FROM zver WHERE fid=$fid);

$qbytes=(
SELECT b.bid
FROM $qver v
INNER JOIN zbytes b ON b.vid=v.vid
);

DELETE FROM zbytes ON SELECT * FROM $qbytes;
DELETE FROM zver ON SELECT * FROM $qver;
DELETE FROM zfile ON SELECT * FROM $qfile;
