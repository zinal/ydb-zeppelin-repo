DECLARE $vid AS Utf8;
DECLARE $fid AS Utf8;
DECLARE $frozen AS Bool;
DECLARE $tv AS Timestamp;
DECLARE $author AS Utf8;
DECLARE $message AS Utf8;

UPSERT INTO zver(vid,fid,frozen,tv,author,message)
VALUES($vid,$fid,$frozen,$tv,$author,$message);
