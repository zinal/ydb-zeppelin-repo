DECLARE $fparent AS Utf8;
DECLARE $fname AS Utf8;

SELECT v.fid, v.vid, v.frozen
FROM (SELECT fid, vid FROM zfile VIEW naming
      WHERE fparent=$fparent AND fname=$fname) AS f
INNER JOIN zver v ON v.fid=f.fid AND v.vid=f.vid;
