DECLARE $fparent AS Utf8;
DECLARE $fname AS Utf8;

SELECT v.fid, v.vid, v.frozen
FROM (SELECT fid FROM zfile_name
      WHERE fparent=$fparent AND fname=$fname) AS fn
INNER JOIN zfile f ON f.fid=fn.fid
INNER JOIN zver v ON v.fid=f.fid AND v.vid=f.vid;
