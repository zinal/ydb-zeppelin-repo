DECLARE $fid AS Utf8;
DECLARE $fparent AS Utf8;
DECLARE $fname AS Utf8;

DELETE FROM zfile_name
WHERE fparent=$fparent AND fname=$fname;

UPSERT INTO zfile_name (fid, fparent, fname)
VALUES ($fid, $fparent, $fname);

UPSERT INTO zfile (fid, fparent, fname)
VALUES ($fid, $fparent, $fname);
