DECLARE fid AS Utf8;

SELECT vid, tv, author, message
FROM zver
WHERE fid=$fid;
