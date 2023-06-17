DECLARE $did AS Utf8;
DECLARE $dparent_old AS Utf8;
DECLARE $dparent_new AS Utf8;
DECLARE $dname_old AS Utf8;
DECLARE $dname_new AS Utf8;

DELETE FROM zdir ON
SELECT $dparent_old AS dparent, $dname_old AS dname;

UPSERT INTO zdir (did, dparent, dname)
VALUES ($did, $dparent_new, $dname_new);
