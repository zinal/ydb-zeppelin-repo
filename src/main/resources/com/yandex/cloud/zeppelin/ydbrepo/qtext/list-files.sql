DECLARE $did AS Utf8;

SELECT fid FROM zfile VIEW naming WHERE fparent=$did;
