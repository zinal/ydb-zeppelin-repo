DECLARE $fid AS Utf8;

$qfile=(SELECT fid FROM zfile WHERE fid=$fid);

$qfile_name=(
SELECT fn.fparent AS fparent, fn.fname AS fname
FROM (SELECT fparent, fname FROM zfile WHERE fid=$fid) AS f
INNER JOIN zfile_name fn ON f.fparent=fn.fparent AND f.fname=fn.fname
);

$qver=(SELECT fid, vid FROM zver WHERE fid=$fid);

$qbytes=(
SELECT b.bid AS bid, b.pos AS pos
FROM (SELECT bid FROM zver WHERE fid=$fid) v
INNER JOIN zbytes b ON b.bid=v.bid
);

DELETE FROM zbytes ON SELECT * FROM $qbytes;
DELETE FROM zver ON SELECT * FROM $qver;
DELETE FROM zfile_name ON SELECT * FROM $qfile_name;
DELETE FROM zfile ON SELECT * FROM $qfile;
