DECLARE $fid AS Utf8;

SELECT f.fparent, f.fname, v.vid, v.frozen
FROM (SELECT vid,fparent,fname FROM zfile WHERE fid=$fid) AS f
INNER JOIN zver v ON v.fid=f.fid AND v.vid=f.vid;
