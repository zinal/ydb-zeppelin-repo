DECLARE $did AS Utf8;

SELECT did FROM zdir VIEW naming WHERE dparent=$did;
