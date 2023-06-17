DECLARE $dparent AS Utf8;

SELECT did, dname FROM zdir WHERE dparent=$dparent;
