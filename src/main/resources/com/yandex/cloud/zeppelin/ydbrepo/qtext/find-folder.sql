DECLARE $dname AS Utf8;
DECLARE $dparent AS Utf8;

SELECT did FROM zdir VIEW naming
WHERE dparent=$dparent AND dname=$dname;
