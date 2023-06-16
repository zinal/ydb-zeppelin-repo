DECLARE $fid AS Utf8;
DECLARE $vid AS Utf8;

$qbytes=(
SELECT x.bid AS bid, x.pos AS pos FROM
  (SELECT bid FROM zver WHERE fid=$fid AND vid=$vid) AS v
  INNER JOIN zbytes x ON x.bid=v.bid
);

DELETE FROM zbytes ON SELECT * FROM $qbytes;
