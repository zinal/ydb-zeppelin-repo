DECLARE $did AS Utf8;
DECLARE $dname AS Utf8;
DECLARE $dparent AS Utf8;

UPSERT INTO zdir(did,dname,dparent)
VALUES($did,$dname,$dparent);
