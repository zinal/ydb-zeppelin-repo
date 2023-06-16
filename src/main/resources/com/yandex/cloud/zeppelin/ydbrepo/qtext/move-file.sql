DECLARE $fid AS Utf8;
DECLARE $fparent AS Utf8;
DECLARE $fname AS Utf8;

UPSERT INTO zfile (fid, fparent, fname)
VALUES ($fid, $fparent, $fname);
