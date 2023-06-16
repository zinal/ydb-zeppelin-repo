DECLARE $srcId AS Utf8;
DECLARE $dstId AS Utf8;
DECLARE $name AS Utf8;

UPSERT INTO zdir (did, dparent, dname)
VALUES ($srcId, $dstId, $name);
